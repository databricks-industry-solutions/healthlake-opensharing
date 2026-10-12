import base64
import hashlib
import io
import json
import os
import posixpath
import re
import resource
import threading
import time
import traceback
import urllib.parse
import uuid
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import urllib3
from botocore.auth import S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError
from databricks.sdk.core import Config as DatabricksConfig
from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastavro import reader as avro_reader
from starlette.concurrency import run_in_threadpool

REGION = os.environ["AWS_REGION"]
ROLE_ARNS = [r.strip() for r in os.environ["AWS_ROLE_ARNS"].split(",") if r.strip()]
# "share": one share (so one Unity Catalog catalog) per data store, which scales to
# hundreds of stores. "schema": one share with a schema per store; Unity Catalog kept
# only ~5,000 tables of a shared catalog in testing, so that is ~45 stores at most.
LAYOUT = os.environ.get("LAYOUT", "share")
SHARE = "healthlake"
SCHEMA = "fhir"
ALLOWED_CALLERS = {c.strip() for c in os.environ.get("ALLOWED_CALLERS", "").split(",") if c.strip()}
URL_TTL_SECONDS = 3600
# The Delta Sharing client refreshes URLs that have less than 15 minutes left, so
# new URLs need well over that or every refresh comes back already "expiring".
MIN_URL_SECONDS = 1800
DISCOVERY_TTL_SECONDS = 300
LINK_SUFFIX = "_healthlake_view"
MAX_NAME_LENGTH = 255  # Unity Catalog's limit for share and schema names
NAMESPACE = uuid.UUID("2f34a9f2-0000-4000-8000-000000000000")

if LAYOUT not in ("schema", "share"):
    raise ValueError(f"LAYOUT must be 'schema' or 'share', not '{LAYOUT}'")


def parse_stores(value):
    stores = {}
    if value.strip() == "*":  # every ACTIVE data store
        return stores
    for entry in filter(None, (e.strip() for e in value.split(","))):
        alias, _, source = entry.partition("=")
        stores[alias.strip()] = (source or alias).strip()
    return stores


STORES = parse_stores(os.environ.get("STORES", "*"))

app = FastAPI()
# FHIR schemas make responses 100-600 KB of JSON that compresses 20-30x even at level 1
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=1)
sharing = APIRouter(prefix="/delta-sharing")
databricks_config = DatabricksConfig(product="healthlake-opensharing", product_version="0.1.0")


class LRU:
    def __init__(self, size):
        self.size = size
        self.items = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            hit = self.items.get(key)
            if hit is None:
                return None
            expires, value = hit
            if time.time() >= expires:
                del self.items[key]
                return None
            self.items.move_to_end(key)
            return value

    def put(self, key, value, expires=float("inf")):
        with self.lock:
            self.items[key] = (expires, value)
            self.items.move_to_end(key)
            while len(self.items) > self.size:
                self.items.popitem(last=False)


_aws_lock = threading.Lock()
_aws = {}
_discovery_lock = threading.Lock()
_stores = {"at": 0.0, "items": None}
_tables = LRU(4096)
_creds = LRU(4096)
# Iceberg metadata files are write-once, so a snapshot keyed by its metadata
# location never goes stale; only the location lookup decides freshness.
_snapshots = LRU(2048)
_http = urllib3.PoolManager(maxsize=32, timeout=urllib3.Timeout(connect=5, read=60))


class VendingDenied(Exception):
    pass


class BadRequest(Exception):
    pass


class NotFound(Exception):
    pass


class Unsupported(Exception):
    pass


def databricks_token():
    return databricks_config.authenticate()["Authorization"].split(" ", 1)[1]


def token_claims(token):
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=="))
    return {k: claims.get(k) for k in ("iss", "aud", "sub")}


# The app's own Databricks OAuth token is the web identity: each role trusts
# this workspace's OIDC issuer for the app service principal only, so no AWS
# secret exists anywhere. One role per AWS account that holds data stores.
def aws(role_arn):
    with _aws_lock:
        hit = _aws.get(role_arn)
        if hit and time.time() < hit["expires"] - 300:
            return hit
        creds = boto3.client("sts", region_name=REGION).assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName="databricks-app-sharing",
            WebIdentityToken=databricks_token(),
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=REGION,
        )
        _aws[role_arn] = {
            "expires": creds["Expiration"].timestamp(),
            "glue": session.client("glue"),
            "lakeformation": session.client("lakeformation"),
            "healthlake": session.client("healthlake"),
        }
        return _aws[role_arn]


def normalize(name):
    return re.sub(r"[^a-z0-9_]", "_", name.lower())


def datastores(role):
    healthlake, token = aws(role)["healthlake"], None
    while True:
        kwargs = {"Filter": {"DatastoreStatus": "ACTIVE"}, **({"NextToken": token} if token else {})}
        response = healthlake.list_fhir_datastores(**kwargs)
        yield from response["DatastorePropertiesList"]
        token = response.get("NextToken")
        if not token:
            return


# Data store IDs never repeat, so a name that ends in one always points at the
# same data store, even if another account later creates one with the same name.
def default_name(datastore_name, datastore_id):
    prefix = normalize(datastore_name)[:MAX_NAME_LENGTH - len(datastore_id) - 1]
    return f"{prefix}_{datastore_id}" if prefix else datastore_id


def configured_name(alias, source):
    if alias != source:
        return normalize(alias)
    if source.endswith(LINK_SUFFIX):
        return normalize(source[:-len(LINK_SUFFIX)])
    return None  # a bare data store ID: <data store name>_<id>, known once listed


# Checked from the setting alone, before any AWS call, so an account that fails to
# list can never make a duplicate name look unique for a while. A bare ID is served
# as <name>_<id> or <id>, so it clashes with any other name that ends in it.
def clashing_entries():
    names = {alias: configured_name(alias, source) for alias, source in STORES.items()}
    fixed = {alias: name for alias, name in names.items() if name is not None}
    counts = Counter(fixed.values())
    clashes = {alias for alias, name in fixed.items() if counts[name] > 1}
    for alias, source in STORES.items():
        if names[alias] is None:
            ends_in_id = {a for a, n in fixed.items() if n == source or n.endswith(f"_{source}")}
            if ends_in_id:
                clashes |= ends_in_id | {alias}
    if clashes:
        print(f"WARNING: STORES entries {', '.join(sorted(clashes))} give the same name; none of them is served "
              "until each name is unique")
    return clashes


# (name, resource link, roles to try). Listing Glue databases is far too slow in
# accounts with many of them, so data stores come from the HealthLake API and
# HealthLake's naming convention gives the link: <name>_<datastore id>_healthlake_view.
# STORES entries point at a data store ID or a resource link rather than a data
# store name, which another account could reuse.
def candidates(failed):
    clashes = clashing_entries()
    selected = []
    for alias, link in STORES.items():
        if link.endswith(LINK_SUFFIX) and alias not in clashes:
            name = configured_name(alias, link)
            if not name:
                print(f"WARNING: STORES entry '{alias}' gives an empty name; name it with <name>={link}")
                continue
            selected.append((name, link, ROLE_ARNS))
    if STORES and all(link.endswith(LINK_SUFFIX) for link in STORES.values()):
        return selected
    found, errors = {}, []
    for role in ROLE_ARNS:
        try:
            for ds in datastores(role):
                name, ds_id = ds.get("DatastoreName") or "", ds["DatastoreId"]
                found[ds_id] = (default_name(name, ds_id), f"{normalize(name)}_{ds_id}{LINK_SUFFIX}", [role])
        except Exception as e:
            print(f"WARNING: role {role} could not list HealthLake data stores: {e}")
            failed.add(role)
            errors.append(e)
    if errors and len(errors) == len(ROLE_ARNS):
        raise errors[-1]  # nothing to go on: the previous list stays, and the next request retries
    if not STORES:
        return list(found.values())
    for alias, source in STORES.items():
        if source.endswith(LINK_SUFFIX) or alias in clashes:
            continue
        if source not in found:
            print(f"WARNING: STORES entry '{alias}={source}' is not the ID of an ACTIVE data store; "
                  "entries must name a data store ID or a resource link")
            continue
        name, link, roles = found[source]
        selected.append((configured_name(alias, source) or name, link, roles))
    return selected


def resolve(name, link, roles, failed):
    for role in roles:
        if role not in failed:
            try:
                glue = aws(role)["glue"]
            except Exception as e:
                print(f"WARNING: could not assume role {role}: {e}")
                failed.add(role)
        if role in failed:
            # Roles are tried in order, so a later account only answers for this link
            # once every earlier one has; until then the name keeps what it served.
            return (_stores["items"] or {}).get(name)
        try:
            database = glue.get_database(Name=link)["Database"]
        except (ClientError, BotoCoreError) as e:
            code = e.response["Error"]["Code"] if isinstance(e, ClientError) else None
            if code == "AccessDeniedException":  # data store exists but this role has no grant on it yet
                print(f"WARNING: skipping data store '{name}': no Lake Formation DESCRIBE on '{link}'")
                return None
            if code == "EntityNotFoundException":
                continue
            # Throttling or a timeout says nothing about the data store, so it keeps
            # what it served before and the other data stores are still discovered.
            print(f"WARNING: could not look up '{link}' for data store '{name}', keeping what it served before: {e}")
            return (_stores["items"] or {}).get(name)
        target = database.get("TargetDatabase")
        if not target:
            print(f"WARNING: skipping '{name}': Glue database '{link}' is not a HealthLake resource link")
            return None
        return {"name": name, "role": role, "link_db": link,
                "service_account": target["CatalogId"], "target_db": target["DatabaseName"]}
    print(f"WARNING: no Glue resource link '{link}' for data store '{name}'; "
          "set STORES=<name>=<resource link> to point at it explicitly")
    return None


def discover():
    failed = set()
    found = candidates(failed)
    with ThreadPoolExecutor(16) as pool:
        resolved = [s for s in pool.map(lambda c: resolve(*c, failed), found) if s]
    served = {s["name"]: s for s in resolved}
    # A role that could not be reached this time tells us nothing about its data
    # stores, so it keeps serving what it served before; one account's outage or
    # misconfiguration never hides the other accounts' data.
    for name, store in (_stores["items"] or {}).items():
        if store["role"] in failed:
            served.setdefault(name, store)
    return served


# The first call waits for discovery; after that a stale list keeps being served
# while one background thread refreshes it, so no request waits on discovery.
def stores():
    if _stores["items"] is None:
        with _discovery_lock:
            if _stores["items"] is None:
                _stores.update(at=time.time(), items=discover())
    elif time.time() - _stores["at"] > DISCOVERY_TTL_SECONDS and _discovery_lock.acquire(blocking=False):
        def refresh():
            try:
                _stores.update(at=time.time(), items=discover())
            except Exception as e:
                _stores["at"] = time.time()  # retry after the usual interval, not on every request
                print(f"WARNING: data store discovery failed, keeping the previous list: {e}")
            finally:
                _discovery_lock.release()
        threading.Thread(target=refresh, daemon=True).start()
    return _stores["items"]


def namespace_of(store):
    return (store["name"], SCHEMA) if LAYOUT == "share" else (SHARE, store["name"])


def namespaces():
    return {namespace_of(s): s for s in stores().values()}


def stable_id(*parts):
    return str(uuid.uuid5(NAMESPACE, ".".join(parts)))


def table_item(store, table):
    share, schema = namespace_of(store)
    return {"name": table, "schema": schema, "share": share,
            "shareId": stable_id(share), "id": stable_id(share, schema, table)}


def list_tables(store):
    names = _tables.get(store["name"])
    if names is None:
        names = []
        for page in aws(store["role"])["glue"].get_paginator("get_tables").paginate(DatabaseName=store["link_db"]):
            for t in page["TableList"]:
                if (t.get("Parameters") or {}).get("metadata_location"):
                    names.append(t["Name"])
        names = sorted(names)
        _tables.put(store["name"], names, time.time() + DISCOVERY_TTL_SECONDS)
    return names


# Only credentials are cached per table: a boto3 S3 client per table costs about
# 470 KB, which adds up to gigabytes across hundreds of data stores.
def vended_credentials(store, table):
    # keyed by the table itself, so a re-pointed resource link cannot reuse another table's credentials
    key = (store["service_account"], store["target_db"], table)
    hit = _creds.get(key)
    if hit:
        return hit
    try:
        creds = aws(store["role"])["lakeformation"].get_temporary_glue_table_credentials(
            TableArn=f"arn:aws:glue:{REGION}:{store['service_account']}:table/{store['target_db']}/{table}",
            SupportedPermissionTypes=["TABLE_PERMISSION"],
            DurationSeconds=URL_TTL_SECONDS,
        )
    except ClientError as e:
        if "AccessDenied" in str(e):
            raise VendingDenied(
                f"Lake Formation refused to vend credentials for table '{table}' in data store "
                f"'{store['name']}'. Either the app role lacks LF grants, or full-table external "
                "data access is not enabled in this account's Lake Formation settings."
            ) from e
        raise
    credentials = Credentials(creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"])
    expires = creds["Expiration"].timestamp()
    # A presigned URL stops working when the credentials that signed it expire,
    # so stop handing these out while they can still sign long-lived URLs.
    refresh_at = expires - min(MIN_URL_SECONDS, (expires - time.time()) / 2)
    _creds.put(key, (credentials, expires), refresh_at)
    return credentials, expires


def presign(credentials, path, expires_in):
    p = urllib.parse.urlparse(path)
    url = f"https://{p.netloc}.s3.{REGION}.amazonaws.com/{urllib.parse.quote(p.path.lstrip('/'), safe='/~')}"
    request = AWSRequest(method="GET", url=url)
    # SigV4 query auth; the KMS-encrypted HealthLake bucket rejects SigV2
    S3SigV4QueryAuth(credentials, "s3", REGION, expires=expires_in).add_auth(request)
    return request.url


def s3_get(credentials, path):
    response = _http.request("GET", presign(credentials, path, 300))
    if response.status != 200:
        raise ClientError({"Error": {"Code": str(response.status), "Message": response.data[:300].decode(errors="replace")}},
                          "GetObject")
    return response.data


def read_snapshot(credentials, metadata_location):
    meta = json.loads(s3_get(credentials, metadata_location))
    if "schemas" in meta:
        schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    else:
        schema = meta["schema"]
    snap_id = meta.get("current-snapshot-id", -1)
    snap = next((s for s in meta.get("snapshots", []) if s["snapshot-id"] == snap_id), None)
    if snap is None:  # table exists but has no committed data yet
        return schema, [], 0
    if "manifest-list" not in snap:  # Iceberg v1 also allows listing manifests inline
        raise Unsupported("this table's snapshot lists its manifests inline instead of in a manifest list, "
                          "which the server does not support")
    files = []
    for m in avro_reader(io.BytesIO(s3_get(credentials, snap["manifest-list"]))):
        for entry in avro_reader(io.BytesIO(s3_get(credentials, m["manifest_path"]))):
            if entry["status"] == 2:  # DELETED
                continue
            df = entry["data_file"]
            # Serving only the data files of a merge-on-read table would return rows it
            # has deleted. HealthLake writes Iceberg v1, which has no delete files.
            if df.get("content", 0) != 0:
                raise Unsupported("this table uses Iceberg delete files (merge-on-read), "
                                  "which the server does not support")
            files.append((df["file_path"], df["file_size_in_bytes"]))
    # The commit time is the table version: HealthLake writes Iceberg v1 tables, which
    # have no sequence numbers, and a count of snapshots repeats once old ones expire.
    return schema, files, snap["timestamp-ms"]


def metadata_location(store, table):
    return aws(store["role"])["glue"].get_table(
        DatabaseName=store["link_db"], Name=table
    )["Table"]["Parameters"]["metadata_location"]


def snapshot(store, table, location=None):
    location = location or metadata_location(store, table)
    key = (store["name"], table, location)
    hit = _snapshots.get(key)
    if hit:
        return hit
    credentials, _ = vended_credentials(store, table)
    schema, files, version = read_snapshot(credentials, location)
    # Encoding the nested FHIR schema costs more than everything else in a
    # request, so it happens once per snapshot rather than once per call.
    header = "".join(json.dumps(line) + "\n" for line in table_header(store, table, schema, version))
    snap = {"location": location, "files": files, "version": version, "header": header}
    _snapshots.put(key, snap)
    return snap


def iceberg_to_spark(t):
    if isinstance(t, str):
        if t.startswith("decimal"):
            return t
        if t.startswith("fixed"):
            return "binary"
        return {
            "boolean": "boolean", "int": "integer", "long": "long",
            "float": "float", "double": "double", "date": "date",
            "time": "string", "timestamp": "timestamp_ntz",
            "timestamptz": "timestamp", "string": "string",
            "uuid": "string", "binary": "binary",
        }[t]
    if t["type"] == "struct":
        return {"type": "struct", "fields": [
            {"name": f["name"], "type": iceberg_to_spark(f["type"]),
             "nullable": not f["required"], "metadata": {}}
            for f in t["fields"]]}
    if t["type"] == "list":
        return {"type": "array", "elementType": iceberg_to_spark(t["element"]),
                "containsNull": not t["element-required"]}
    if t["type"] == "map":
        return {"type": "map", "keyType": iceberg_to_spark(t["key"]),
                "valueType": iceberg_to_spark(t["value"]),
                "valueContainsNull": not t["value-required"]}
    raise ValueError(f"unsupported iceberg type: {t}")


def table_header(store, table, schema, version):
    spark_schema = iceberg_to_spark({"type": "struct", "fields": schema["fields"]})
    return [
        {"protocol": {"minReaderVersion": 1}},
        {"metaData": {
            "id": table_item(store, table)["id"],
            "name": table,
            "format": {"provider": "parquet"},
            "schemaString": json.dumps(spark_schema),
            "partitionColumns": [],
            "configuration": {},
            "version": version,
        }},
    ]


def encode_token(data):
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def decode_token(token, kind="pageToken"):
    try:
        data = json.loads(base64.urlsafe_b64decode(token + "=="))
        if not isinstance(data, dict):
            raise TypeError
        return data
    except (ValueError, TypeError) as e:
        raise BadRequest(f"{kind} is not valid") from e


def page(items, params):
    try:
        max_results = int(params.get("maxResults") or 0)
        start = int(decode_token(params["pageToken"])["o"]) if params.get("pageToken") else 0
    except (KeyError, TypeError, ValueError) as e:
        raise BadRequest("maxResults or pageToken is not valid") from e
    if start < 0:
        raise BadRequest("pageToken is not valid")
    end = start + max_results if max_results > 0 else len(items)
    result = {"items": items[start:end]}
    if end < len(items):
        result["nextPageToken"] = encode_token({"o": end})
    return result


# Page and refresh tokens carry the metadata location of the snapshot a query
# started from, so later pages and URL refreshes list the same files even if a
# new snapshot lands mid-query.
def token_location(data, current_location, kind):
    location = str(data.get("m", ""))
    # metadata files of one table share a directory; anything else is not this table
    if posixpath.dirname(location) != posixpath.dirname(current_location) or \
            not location.endswith(".metadata.json"):
        raise BadRequest(f"{kind} does not belong to this table")
    return location


def decode_file_token(token, current_location):
    data = decode_token(token)
    try:
        offset = int(data["o"])
    except (KeyError, TypeError, ValueError) as e:
        raise BadRequest("pageToken is not valid") from e
    if offset < 0:
        raise BadRequest("pageToken is not valid")
    return offset, token_location(data, current_location, "pageToken")


def decode_refresh_token(token, current_location):
    return token_location(decode_token(token, "refreshToken"), current_location, "refreshToken")


def parse_timestamp(value):
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise BadRequest(f"timestamp '{value}' is not ISO 8601") from e
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int(ts.timestamp() * 1000)


# Only the current snapshot is served. A request for any other point in time
# fails loudly instead of silently returning today's data.
def check_current(snap, version=None, timestamp=None):
    try:
        version = None if version is None else int(version)
    except (TypeError, ValueError) as e:
        raise BadRequest(f"version '{version}' is not an integer") from e
    if version is not None and version != snap["version"]:
        raise BadRequest(f"version {version} requested, but only the current version "
                         f"({snap['version']}) is served; time travel is not supported")
    if timestamp is not None and parse_timestamp(timestamp) < snap["version"]:
        raise BadRequest(f"timestamp {timestamp} is before the current snapshot; "
                         "time travel is not supported")


def not_found(message):
    return JSONResponse({"errorCode": "NOT_FOUND", "message": message}, status_code=404)


def ndjson(body, version, end_stream_action=False):
    # the client sends delta-sharing-capabilities with accepted responseformats;
    # the server must echo its choice or the client may parse as delta format
    capabilities = "responseformat=parquet" + (";includeendstreamaction=true" if end_stream_action else "")
    return Response(
        body,
        media_type="application/x-ndjson; charset=utf-8",
        headers={"Delta-Table-Version": str(version), "delta-sharing-capabilities": capabilities},
    )


def share_names():
    return sorted({share for share, _ in namespaces()})


def find_store(share, schema):
    store = namespaces().get((share, schema))
    if store is None:
        raise NotFound(f"schema {share}.{schema} not found")
    return store


def find_table(share, schema, table):
    store = find_store(share, schema)
    if table not in list_tables(store):
        raise NotFound(f"table {share}.{schema}.{table} not found")
    return store


@app.exception_handler(NotFound)
def missing(_request, e):
    return not_found(str(e))


@app.exception_handler(VendingDenied)
def vending_denied(_request, e):
    print(f"VENDING DENIED: {e}")
    return JSONResponse({"errorCode": "LAKE_FORMATION_ACCESS_DENIED", "message": str(e)}, status_code=403)


@app.exception_handler(BadRequest)
def bad_request(_request, e):
    return JSONResponse({"errorCode": "INVALID_PARAMETER_VALUE", "message": str(e)}, status_code=400)


@app.exception_handler(ClientError)
def aws_error(_request, e):
    print(f"AWS ERROR: {e}")
    return JSONResponse({"errorCode": "AWS_ERROR", "message": str(e)}, status_code=502)


# 4xx, not 5xx: Delta Sharing clients retry 5xx for minutes, and this never changes on retry
@app.exception_handler(Unsupported)
def unsupported(_request, e):
    return JSONResponse({"errorCode": "UNSUPPORTED_TABLE", "message": str(e)}, status_code=400)


# Anyone with CAN_USE on the app passes the Apps gateway, and presigned URLs
# bypass Unity Catalog grants, so only the configured recipients get through.
@app.middleware("http")
async def authorize_and_log(request: Request, call_next):
    caller = request.headers.get("x-forwarded-email", "-")
    started = time.perf_counter()
    if caller in ALLOWED_CALLERS:
        try:
            response = await call_next(request)
        except Exception:  # errors without a handler above; still answer in JSON and log the request
            traceback.print_exc()
            response = JSONResponse({"errorCode": "INTERNAL_ERROR",
                                     "message": "unexpected server error; see the app logs"}, status_code=500)
    else:
        response = JSONResponse({"errorCode": "PERMISSION_DENIED",
                                 "message": f"caller {caller} is not an allowed recipient"},
                                status_code=403)
    query = f"?{request.url.query}" if request.url.query else ""
    print(f"{request.method} {request.url.path}{query} {response.status_code} "
          f"{(time.perf_counter() - started) * 1000:.0f}ms caller={caller} "
          f"ua={request.headers.get('user-agent', '-')[:60]}")
    return response


@app.on_event("startup")
def log_identity():
    print(f"DATABRICKS_IDENTITY {json.dumps(token_claims(databricks_token()))}")
    if not ALLOWED_CALLERS:
        print("WARNING: ALLOWED_CALLERS is empty; every request will be rejected")


# the Apps gateway answers /healthz itself, so it never reaches the app
@app.get("/status")
def status():
    return {"ok": True, "databricks_identity": token_claims(databricks_token()),
            "aws_roles": ROLE_ARNS, "layout": LAYOUT, "stores": sorted(stores()),
            "cached_snapshots": len(_snapshots.items),
            "peak_memory_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024}


@sharing.get("/shares")
def list_shares(request: Request):
    return page([{"name": s, "id": stable_id(s)} for s in share_names()], request.query_params)


@sharing.get("/shares/{share}")
def get_share(share: str):
    if share not in share_names():
        return not_found(f"share {share} not found")
    return {"share": {"name": share, "id": stable_id(share)}}


@sharing.get("/shares/{share}/schemas")
def list_schemas(share: str, request: Request):
    if share not in share_names():
        return not_found(f"share {share} not found")
    schemas = sorted(schema for sh, schema in namespaces() if sh == share)
    return page([{"name": s, "share": share} for s in schemas], request.query_params)


@sharing.get("/shares/{share}/all-tables")
def list_all_tables(share: str, request: Request):
    if share not in share_names():
        return not_found(f"share {share} not found")
    in_share = [store for (sh, _), store in sorted(namespaces().items()) if sh == share]
    with ThreadPoolExecutor(16) as pool:
        tables = list(pool.map(list_tables, in_share))
    items = [table_item(store, t) for store, names in zip(in_share, tables) for t in names]
    return page(items, request.query_params)


@sharing.get("/shares/{share}/schemas/{schema}/tables")
def list_schema_tables(share: str, schema: str, request: Request):
    store = find_store(share, schema)
    return page([table_item(store, t) for t in list_tables(store)], request.query_params)


@sharing.api_route("/shares/{share}/schemas/{schema}/tables/{table}/version", methods=["GET", "HEAD"])
def table_version(share: str, schema: str, table: str, startingTimestamp: str | None = None):
    store = find_table(share, schema, table)
    snap = snapshot(store, table)
    check_current(snap, timestamp=startingTimestamp)
    return Response(headers={"Delta-Table-Version": str(snap["version"]),
                             "delta-sharing-capabilities": "responseformat=parquet"})


@sharing.get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
def table_metadata(share: str, schema: str, table: str):
    store = find_table(share, schema, table)
    snap = snapshot(store, table)
    return ndjson(snap["header"], snap["version"])


@sharing.get("/shares/{share}/schemas/{schema}/tables/{table}/changes")
def table_changes(share: str, schema: str, table: str):
    raise BadRequest("change data feed is not supported")


def query_files(share, schema, table, body, params, capabilities=""):
    store = find_table(share, schema, table)
    if any(k in body for k in ("startingVersion", "endingVersion", "startingTimestamp")):
        raise BadRequest("change data feed is not supported")
    try:
        max_files = int(body.get("maxFiles") or params.get("maxFiles") or 0)
    except (TypeError, ValueError) as e:
        raise BadRequest("maxFiles must be an integer") from e
    page_token = body.get("pageToken") or params.get("pageToken")
    refresh_token = body.get("refreshToken")

    current = metadata_location(store, table)
    if page_token:
        offset, location = decode_file_token(page_token, current)
    else:
        offset, location = 0, decode_refresh_token(refresh_token, current) if refresh_token else current
    snap = snapshot(store, table, location)
    check_current(snap, version=body.get("version"), timestamp=body.get("timestamp"))

    files = snap["files"]
    end = offset + max_files if max_files > 0 else len(files)
    credentials, creds_expire = vended_credentials(store, table)
    expires_in = max(60, int(min(URL_TTL_SECONDS, creds_expire - time.time() - 60)))
    expiry_ms = int((time.time() + expires_in) * 1000)
    lines = []
    for file_path, size in files[offset:end]:
        lines.append({"file": {
            "url": presign(credentials, file_path, expires_in),
            "id": hashlib.md5(file_path.encode()).hexdigest(),
            "partitionValues": {},
            "size": size,
            "expirationTimestamp": expiry_ms,
        }})
    # The protocol's last line: the next page, and a refresh token that keeps the
    # client's URL refreshes on this snapshot. Clients that ask for neither, and do
    # not page, get the plain file list.
    end_stream = {"minUrlExpirationTimestamp": expiry_ms}
    if body.get("includeRefreshToken") is True or refresh_token:
        end_stream["refreshToken"] = encode_token({"m": location})
    if end < len(files):
        end_stream["nextPageToken"] = encode_token({"o": end, "m": location})
    requested = "includeendstreamaction=true" in capabilities.lower().replace(" ", "")
    if requested or max_files > 0 or len(end_stream) > 1:
        lines.append({"endStreamAction": end_stream})
    body_out = snap["header"] + "".join(json.dumps(line) + "\n" for line in lines)
    return ndjson(body_out, snap["version"], end_stream_action=requested)


@sharing.post("/shares/{share}/schemas/{schema}/tables/{table}/query")
async def table_query(share: str, schema: str, table: str, request: Request):
    raw = await request.body()
    try:
        body = json.loads(raw) if raw else {}
    except ValueError as e:
        raise BadRequest("request body is not valid JSON") from e
    if not isinstance(body, dict):
        raise BadRequest("request body must be a JSON object")
    # the rest is blocking AWS I/O; keep it off the event loop
    return await run_in_threadpool(query_files, share, schema, table, body, dict(request.query_params),
                                   request.headers.get("delta-sharing-capabilities", ""))


app.include_router(sharing)
