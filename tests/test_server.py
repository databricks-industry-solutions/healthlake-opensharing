import base64
import datetime
import importlib
import io
import json
import os
import sys
import time
import types
import urllib.parse

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from fastavro import parse_schema, writer
from fastapi.testclient import TestClient

RECIPIENT = "recipient-sp-id"
ALPHA_ID, BETA_ID = "a" * 32, "b" * 32
ALPHA, BETA = f"alpha_{ALPHA_ID}", f"beta_{BETA_ID}"
ALPHA_LINK = f"{ALPHA}_healthlake_view"
BETA_LINK = f"{BETA}_healthlake_view"
TABLE = f"/delta-sharing/shares/healthlake/schemas/{ALPHA}/tables/patient"

MANIFEST_LIST_SCHEMA = parse_schema({
    "type": "record", "name": "manifest_file",
    "fields": [{"name": "manifest_path", "type": "string"}],
})
MANIFEST_SCHEMA = parse_schema({
    "type": "record", "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int"},
        {"name": "data_file", "type": {
            "type": "record", "name": "data_file",
            "fields": [
                {"name": "content", "type": "int"},
                {"name": "file_path", "type": "string"},
                {"name": "file_size_in_bytes", "type": "long"},
            ]}},
    ],
})


def avro(schema, records):
    buf = io.BytesIO()
    writer(buf, schema, records)
    return buf.getvalue()


class FakeHTTP:
    """Serves signed S3 GETs: https://<bucket>.s3.<region>.amazonaws.com/<key>?X-Amz-..."""

    def __init__(self, objects):
        self.objects = objects
        self.gets = 0

    def request(self, method, url):
        self.gets += 1
        parsed = urllib.parse.urlparse(url)
        assert "X-Amz-Signature" in parsed.query and "X-Amz-Security-Token" in parsed.query
        path = f"s3://{parsed.netloc.split('.s3.')[0]}{urllib.parse.unquote(parsed.path)}"
        body = self.objects.get(path)
        return types.SimpleNamespace(status=200 if body is not None else 404, data=body or b"NoSuchKey")


class FakeGlue:
    """One AWS account: resource-link databases, each with Iceberg tables."""

    def __init__(self, databases):
        self.databases = databases  # {link_db: {"target": (account, db), "tables": {name: location}}}
        self.get_table_calls = 0

    def get_table(self, DatabaseName, Name):
        self.get_table_calls += 1
        return {"Table": {"Parameters": {"metadata_location": self.databases[DatabaseName]["tables"][Name]}}}

    def get_database(self, Name):
        if Name not in self.databases:
            raise ClientError({"Error": {"Code": "EntityNotFoundException", "Message": "not found"}}, "GetDatabase")
        if self.databases[Name]["target"] is None:  # a plain Glue database, not a resource link
            return {"Database": {"Name": Name}}
        account, db = self.databases[Name]["target"]
        return {"Database": {"Name": Name, "TargetDatabase": {"CatalogId": account, "DatabaseName": db}}}

    def get_paginator(self, name):
        glue = self

        class Paginator:
            def paginate(self, DatabaseName):
                tables = glue.databases[DatabaseName]["tables"]
                yield {"TableList": [{"Name": t, "Parameters": {"metadata_location": loc}}
                                     for t, loc in tables.items()] + [{"Name": "not_iceberg", "Parameters": {}}]}
        return Paginator()


class FakeHealthLake:
    def __init__(self, datastores):
        self.datastores = datastores
        self.calls = 0
        self.fail = False

    def list_fhir_datastores(self, Filter, NextToken=None):
        self.calls += 1
        if self.fail:
            raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "ListFHIRDatastores")
        if NextToken is None and len(self.datastores) > 1:
            return {"DatastorePropertiesList": self.datastores[:1], "NextToken": "page2"}
        return {"DatastorePropertiesList": self.datastores[1:] if NextToken else self.datastores}


class FakeLakeFormation:
    def __init__(self):
        self.deny = False
        self.lifetime = 3600
        self.calls = []

    def get_temporary_glue_table_credentials(self, TableArn, SupportedPermissionTypes, DurationSeconds):
        self.calls.append(TableArn)
        self.duration = DurationSeconds
        if self.deny:
            raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "AccessDenied"}},
                              "GetTemporaryGlueTableCredentials")
        expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=self.lifetime)
        return {"AccessKeyId": "a", "SecretAccessKey": "s", "SessionToken": "t", "Expiration": expiration}


def add_snapshot(objects, table_dir, name, files, sequence_number, timestamp_ms, delete_files=()):
    bucket = table_dir.split("/")[2]
    manifest = f"{table_dir}/{name}-manifest.avro"
    manifest_list = f"{table_dir}/{name}-manifest-list.avro"
    objects[manifest] = avro(MANIFEST_SCHEMA, [
        {"status": 1, "data_file": {"content": 0, "file_path": f"s3://{bucket}/data/{f}", "file_size_in_bytes": 100}}
        for f in files
    ] + [{"status": 1, "data_file": {"content": 1, "file_path": f"s3://{bucket}/data/{f}", "file_size_in_bytes": 10}}
         for f in delete_files
    ] + [{"status": 2, "data_file": {"content": 0, "file_path": f"s3://{bucket}/data/deleted.parquet",
                                     "file_size_in_bytes": 1}}])
    objects[manifest_list] = avro(MANIFEST_LIST_SCHEMA, [{"manifest_path": manifest}])
    location = f"{table_dir}/{name}.metadata.json"
    # HealthLake writes Iceberg v1: no sequence numbers
    objects[location] = json.dumps({
        "format-version": 1,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": [
            {"id": 1, "name": "id", "required": True, "type": "string"},
            {"id": 2, "name": "name", "required": False, "type": {
                "type": "list", "element-id": 3, "element-required": False, "element": {
                    "type": "struct", "fields": [{"id": 4, "name": "family", "required": False, "type": "string"}]}}},
        ]}],
        "current-snapshot-id": sequence_number,
        "snapshots": [{"snapshot-id": sequence_number, "timestamp-ms": timestamp_ms, "manifest-list": manifest_list}],
    }).encode()
    return location


ALPHA_DIR = "s3://alpha-bucket/patient/metadata"
BETA_DIR = "s3://beta-bucket/patient/metadata"


def make_env(monkeypatch, **overrides):
    settings = {"AWS_REGION": "us-east-1", "AWS_ROLE_ARNS": "role-a,role-b", "ALLOWED_CALLERS": RECIPIENT,
                "LAYOUT": "schema"}
    settings.update(overrides)
    for k in ("STORES",):
        monkeypatch.delenv(k, raising=False)
    for k, v in settings.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
    import databricks.sdk.core
    monkeypatch.setattr(databricks.sdk.core, "Config", lambda **_: None)
    server = importlib.reload(importlib.import_module("app"))

    objects = {}
    alpha_v1 = add_snapshot(objects, ALPHA_DIR, "v1", ["a.parquet", "b.parquet", "c.parquet"], 1, 1_700_000_000_000)
    beta_v1 = add_snapshot(objects, BETA_DIR, "v1", ["z.parquet"], 7, 1_700_000_000_000)
    http = FakeHTTP(objects)
    glue = {
        "role-a": FakeGlue({ALPHA_LINK: {"target": ("111", "alpha_target"), "tables": {"patient": alpha_v1}}}),
        "role-b": FakeGlue({BETA_LINK: {"target": ("222", "beta_target"), "tables": {"patient": beta_v1}}}),
    }
    lf = {"role-a": FakeLakeFormation(), "role-b": FakeLakeFormation()}
    healthlake = {
        "role-a": FakeHealthLake([{"DatastoreName": "alpha", "DatastoreId": ALPHA_ID},
                                  {"DatastoreName": "Not-Linked", "DatastoreId": "c" * 32}]),
        "role-b": FakeHealthLake([{"DatastoreName": "beta", "DatastoreId": BETA_ID}]),
    }
    monkeypatch.setattr(server, "aws", lambda role: {"glue": glue[role], "lakeformation": lf[role],
                                                     "healthlake": healthlake[role]})
    monkeypatch.setattr(server, "_http", http)
    client = TestClient(server.app, headers={"x-forwarded-email": RECIPIENT})
    return {"server": server, "client": client, "s3": http, "glue": glue, "lf": lf,
            "healthlake": healthlake, "objects": objects}


@pytest.fixture
def env(monkeypatch):
    return make_env(monkeypatch)


def query(env, body=None, raw=None, table=TABLE):
    if raw is not None:
        return env["client"].post(f"{table}/query", content=raw, headers={"Content-Type": "application/json"})
    return env["client"].post(f"{table}/query", json=body or {})


def lines(response):
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def file_names(response):
    return [line["file"]["url"].split("?")[0].rsplit("/", 1)[1] for line in lines(response) if "file" in line]


def end_stream(response):
    last = lines(response)[-1]
    return last.get("endStreamAction")


def served(env):
    return sorted(env["server"].discover())


def test_rejects_callers_that_are_not_recipients(env):
    assert env["client"].get("/delta-sharing/shares").status_code == 200
    other = env["client"].get("/delta-sharing/shares", headers={"x-forwarded-email": "someone@example.com"})
    assert other.status_code == 403
    assert other.json()["errorCode"] == "PERMISSION_DENIED"
    assert env["client"].get("/delta-sharing/shares", headers={"x-forwarded-email": ""}).status_code == 403


def test_schema_layout_discovers_stores_across_accounts(env):
    c = env["client"]
    assert c.get("/delta-sharing/shares").json()["items"][0]["name"] == "healthlake"
    assert [s["name"] for s in c.get("/delta-sharing/shares/healthlake/schemas").json()["items"]] == [ALPHA, BETA]
    tables = c.get("/delta-sharing/shares/healthlake/all-tables").json()["items"]
    assert [(t["schema"], t["name"]) for t in tables] == [(ALPHA, "patient"), (BETA, "patient")]
    assert file_names(query(env, table=f"/delta-sharing/shares/healthlake/schemas/{BETA}/tables/patient")) == \
        ["z.parquet"]
    assert env["lf"]["role-b"].calls == ["arn:aws:glue:us-east-1:222:table/beta_target/patient"]


def test_share_layout_is_the_default_and_gives_each_store_its_own_share(monkeypatch):
    env = make_env(monkeypatch, LAYOUT=None)
    assert env["server"].LAYOUT == "share"
    c = env["client"]
    assert [s["name"] for s in c.get("/delta-sharing/shares").json()["items"]] == [ALPHA, BETA]
    assert [s["name"] for s in c.get(f"/delta-sharing/shares/{ALPHA}/schemas").json()["items"]] == ["fhir"]
    assert file_names(query(env, table=f"/delta-sharing/shares/{ALPHA}/schemas/fhir/tables/patient")) == \
        ["a.parquet", "b.parquet", "c.parquet"]
    assert c.get("/delta-sharing/shares/healthlake/schemas").status_code == 404


def test_star_serves_every_data_store(monkeypatch):
    env = make_env(monkeypatch, STORES="*")
    schemas = env["client"].get("/delta-sharing/shares/healthlake/schemas").json()["items"]
    assert [s["name"] for s in schemas] == [ALPHA, BETA]


def test_names_include_the_datastore_id_so_they_never_move(env):
    # a later data store with the same name, in another account, gets its own name
    other = "d" * 32
    env["healthlake"]["role-b"].datastores.append({"DatastoreName": "alpha", "DatastoreId": other})
    env["glue"]["role-b"].databases[f"alpha_{other}_healthlake_view"] = env["glue"]["role-b"].databases[BETA_LINK]
    assert served(env) == [ALPHA, f"alpha_{other}", BETA]
    # and when the first one goes away, its name is not handed to the other
    env["healthlake"]["role-a"].datastores = []
    assert served(env) == [f"alpha_{other}", BETA]


def test_long_names_are_cut_to_fit_and_unnamed_stores_do_not_break_discovery(env):
    server = env["server"]
    assert server.default_name("x" * 300, ALPHA_ID) == "x" * (255 - 33) + "_" + ALPHA_ID
    assert server.default_name("", ALPHA_ID) == ALPHA_ID
    env["healthlake"]["role-a"].datastores.append({"DatastoreId": "e" * 32})  # DatastoreName is optional
    assert served(env) == [ALPHA, BETA]


def test_one_failing_account_does_not_hide_the_others(env):
    env["healthlake"]["role-b"].fail = True
    assert served(env) == [ALPHA]  # cold start: the healthy account is still served
    env["healthlake"]["role-b"].fail = False
    env["server"]._stores["items"] = env["server"].discover()
    env["healthlake"]["role-b"].fail = True
    assert served(env) == [ALPHA, BETA]  # later failures keep what that account served before


def test_when_every_account_fails_discovery_errors_and_retries(env):
    for healthlake in env["healthlake"].values():
        healthlake.fail = True
    assert env["client"].get("/delta-sharing/shares/healthlake/schemas").status_code == 502
    for healthlake in env["healthlake"].values():
        healthlake.fail = False
    schemas = env["client"].get("/delta-sharing/shares/healthlake/schemas").json()["items"]
    assert [s["name"] for s in schemas] == [ALPHA, BETA]  # nothing empty was cached


def test_a_revoked_store_is_not_kept_because_another_account_failed(env):
    env["server"]._stores["items"] = env["server"].discover()
    env["healthlake"]["role-b"].fail = True

    def denied(Name):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "GetDatabase")

    env["glue"]["role-a"].get_database = denied
    assert served(env) == [BETA]


@pytest.mark.parametrize("error", [
    ClientError({"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "GetDatabase"),
    EndpointConnectionError(endpoint_url="https://glue.us-east-1.amazonaws.com"),
])
def test_a_failed_lookup_keeps_that_store_and_still_discovers_the_others(env, error):
    glue = env["glue"]["role-a"]
    lookup = glue.get_database

    def failing(Name):
        raise error

    glue.get_database = failing
    assert served(env) == [BETA]  # cold start: the other stores are still served
    glue.get_database = lookup
    env["server"]._stores["items"] = env["server"].discover()
    glue.get_database = failing
    assert served(env) == [ALPHA, BETA]  # later: it keeps what it served before


def unreachable(monkeypatch, env, bad_role):
    server, real = env["server"], env["server"].aws

    def aws(role):
        if role == bad_role:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "AssumeRoleWithWebIdentity")
        return real(role)

    monkeypatch.setattr(server, "aws", aws)


def test_a_role_that_cannot_be_assumed_does_not_block_the_others(monkeypatch):
    env = make_env(monkeypatch, AWS_ROLE_ARNS="role-bad,role-a,role-b")
    unreachable(monkeypatch, env, "role-bad")
    assert served(env) == [ALPHA, BETA]


def test_a_resource_link_never_moves_to_a_later_account(monkeypatch):
    env = make_env(monkeypatch, STORES="x=custom_healthlake_view")
    server, glue = env["server"], env["glue"]
    glue["role-a"].databases["custom_healthlake_view"] = glue["role-a"].databases[ALPHA_LINK]
    glue["role-b"].databases["custom_healthlake_view"] = glue["role-b"].databases[BETA_LINK]
    server._stores["items"] = server.discover()
    unreachable(monkeypatch, env, "role-a")
    assert server.discover()["x"]["service_account"] == "111"  # keeps account 111, not 222's link
    server._stores["items"] = None
    assert server.discover() == {}  # cold start: waits for role-a rather than guess


def test_a_plain_glue_database_is_skipped(monkeypatch, capsys):
    env = make_env(monkeypatch, STORES=f"{ALPHA_LINK},x=plain_healthlake_view")
    env["glue"]["role-b"].databases["plain_healthlake_view"] = {"target": None, "tables": {}}
    assert served(env) == [ALPHA]
    assert "is not a HealthLake resource link" in capsys.readouterr().out


@pytest.mark.parametrize("stores", [f"clinical={ALPHA_ID},Clinical={BETA_ID}", f"{ALPHA_ID},{ALPHA}={BETA_ID}"])
def test_two_entries_with_the_same_name_serve_neither(monkeypatch, capsys, stores):
    env = make_env(monkeypatch, STORES=stores)
    assert served(env) == []
    env["healthlake"]["role-b"].fail = True  # one of them cannot be listed: the other is still not unique
    assert served(env) == []
    assert "none of them is served until each name is unique" in capsys.readouterr().out


def test_bare_resource_link_names_are_normalized(monkeypatch):
    env = make_env(monkeypatch, STORES="Custom-Link_healthlake_view,_healthlake_view")
    env["glue"]["role-b"].databases["Custom-Link_healthlake_view"] = env["glue"]["role-b"].databases[BETA_LINK]
    assert served(env) == ["custom_link"]


def test_explicit_resource_links_need_no_datastore_listing(monkeypatch):
    env = make_env(monkeypatch, STORES=f"fhir={ALPHA_LINK},other={BETA_LINK}")
    schemas = env["client"].get("/delta-sharing/shares/healthlake/schemas").json()["items"]
    assert [s["name"] for s in schemas] == ["fhir", "other"]
    assert env["healthlake"]["role-a"].calls == env["healthlake"]["role-b"].calls == 0


def test_discovery_refreshes_in_the_background(env):
    server = env["server"]
    assert sorted(server.stores()) == [ALPHA, BETA]
    release = __import__("threading").Event()
    original = server.discover

    def slow_discover():
        release.wait(5)
        return {"gamma": {**original()[ALPHA], "name": "gamma"}}

    server.discover = slow_discover
    server._stores["at"] = 0
    assert sorted(server.stores()) == [ALPHA, BETA]  # stale list served at once
    release.set()
    for _ in range(50):
        if sorted(server.stores()) == ["gamma"]:
            break
        __import__("time").sleep(0.05)
    assert sorted(server.stores()) == ["gamma"]


def test_a_failed_refresh_waits_a_full_interval_before_retrying(env):
    server = env["server"]
    server.stores()
    attempts = []

    def failing():
        attempts.append(1)
        raise RuntimeError("AWS is down")

    server.discover = failing
    server._stores["at"] = 0
    server.stores()
    for _ in range(50):  # the refresh runs in the background and holds the lock until it ends
        if server._discovery_lock.acquire(blocking=False):
            server._discovery_lock.release()
            break
        time.sleep(0.05)
    server.stores()
    server.stores()
    assert attempts == [1]
    assert sorted(server.stores()) == [ALPHA, BETA]  # the previous list is still served


def test_stores_setting_selects_and_renames_by_id_or_link(monkeypatch, capsys):
    env = make_env(monkeypatch, STORES=f"fhir={ALPHA_ID},My-Copy={BETA_LINK},{BETA_ID},missing=nope,byname=alpha")
    schemas = env["client"].get("/delta-sharing/shares/healthlake/schemas").json()["items"]
    assert [s["name"] for s in schemas] == [BETA, "fhir", "my_copy"]
    assert file_names(query(env, table="/delta-sharing/shares/healthlake/schemas/fhir/tables/patient")) == \
        ["a.parquet", "b.parquet", "c.parquet"]
    # a bare data store name could match a different store later, so it is refused
    assert "'byname=alpha' is not the ID of an ACTIVE data store" in capsys.readouterr().out


def test_stores_setting_mixes_ids_and_custom_resource_links(monkeypatch):
    env = make_env(monkeypatch, STORES=f"fhir={ALPHA_ID},other=custom_healthlake_view")
    env["glue"]["role-b"].databases["custom_healthlake_view"] = env["glue"]["role-b"].databases[BETA_LINK]
    assert served(env) == ["fhir", "other"]


def test_list_endpoints_paginate(env):
    c = env["client"]
    first = c.get("/delta-sharing/shares/healthlake/all-tables", params={"maxResults": 1}).json()
    assert len(first["items"]) == 1
    second = c.get("/delta-sharing/shares/healthlake/all-tables",
                   params={"maxResults": 1, "pageToken": first["nextPageToken"]}).json()
    assert second["items"][0]["schema"] == BETA
    assert "nextPageToken" not in second
    assert c.get("/delta-sharing/shares", params={"maxResults": "x"}).status_code == 400
    negative = env["server"].encode_token({"o": -1})
    assert c.get("/delta-sharing/shares/healthlake/all-tables", params={"pageToken": negative}).status_code == 400


def test_metadata_converts_schema_and_is_cached_per_snapshot(env):
    url = f"{TABLE}/metadata"
    first = env["client"].get(url)
    gets_after_first = env["s3"].gets
    second = env["client"].get(url)
    assert first.text == second.text
    assert env["s3"].gets == gets_after_first
    assert first.headers["delta-table-version"] == "1700000000000"
    schema = json.loads(lines(first)[1]["metaData"]["schemaString"])
    assert schema["fields"][1]["type"]["type"] == "array"
    assert env["glue"]["role-a"].get_table_calls == 2


def test_new_snapshot_is_visible_with_a_new_version(env):
    # both metadata files hold one snapshot, so a snapshot count would not change
    url = f"{TABLE}/metadata"
    assert env["client"].get(url).headers["delta-table-version"] == "1700000000000"
    env["glue"]["role-a"].databases[ALPHA_LINK]["tables"]["patient"] = add_snapshot(
        env["objects"], ALPHA_DIR, "v2", ["d.parquet"], 2, 1_800_000_000_000)
    assert env["client"].get(url).headers["delta-table-version"] == "1800000000000"


def test_query_skips_deleted_files_and_bounds_url_expiry_by_credentials(env):
    env["lf"]["role-a"].lifetime = 1200
    response = query(env)
    assert file_names(response) == ["a.parquet", "b.parquet", "c.parquet"]
    first = next(line["file"] for line in lines(response) if "file" in line)
    assert int(urllib.parse.parse_qs(urllib.parse.urlparse(first["url"]).query)["X-Amz-Expires"][0]) <= 1200 - 60
    assert first["url"].startswith("https://alpha-bucket.s3.us-east-1.amazonaws.com/data/a.parquet?")
    now_ms = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
    assert first["expirationTimestamp"] <= now_ms + 1200 * 1000


# 1-hour credentials are cached for 30 minutes, so every URL signed with them has
# over 29 minutes left: more than the client's 15-minute refresh threshold.
@pytest.mark.parametrize("lifetime,cached_for", [(3600, 1800), (1000, 500)])
def test_vended_credentials_are_refreshed_before_urls_get_short(env, lifetime, cached_for):
    env["lf"]["role-a"].lifetime = lifetime
    query(env)
    query(env)
    assert len(env["lf"]["role-a"].calls) == 1
    assert env["lf"]["role-a"].duration == 3600
    refresh_at, _ = env["server"]._creds.items[("111", "alpha_target", "patient")]
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    assert abs(refresh_at - now - cached_for) < 5


def test_pagination_stays_on_the_snapshot_it_started_from(env):
    page1 = query(env, {"maxFiles": 2})
    token = end_stream(page1)["nextPageToken"]
    assert len(file_names(page1)) == 2
    env["glue"]["role-a"].databases[ALPHA_LINK]["tables"]["patient"] = add_snapshot(
        env["objects"], ALPHA_DIR, "v2", ["d.parquet"], 2, 1_800_000_000_000)
    page2 = query(env, {"maxFiles": 2, "pageToken": token})
    assert page2.headers["delta-table-version"] == "1700000000000"
    assert file_names(page2) == ["c.parquet"]
    assert "nextPageToken" not in end_stream(page2)


def test_refresh_token_keeps_url_refreshes_on_the_original_snapshot(env):
    first = query(env, {"includeRefreshToken": True})
    token = end_stream(first)["refreshToken"]
    assert end_stream(first)["minUrlExpirationTimestamp"] == lines(first)[2]["file"]["expirationTimestamp"]
    env["glue"]["role-a"].databases[ALPHA_LINK]["tables"]["patient"] = add_snapshot(
        env["objects"], ALPHA_DIR, "v2", ["d.parquet"], 2, 1_800_000_000_000)
    refreshed = query(env, {"includeRefreshToken": True, "refreshToken": token})
    assert refreshed.headers["delta-table-version"] == "1700000000000"
    assert file_names(refreshed) == ["a.parquet", "b.parquet", "c.parquet"]
    assert file_names(query(env)) == ["d.parquet"]  # a new query sees the new snapshot


def test_end_stream_action_only_when_the_client_can_use_it(env):
    plain = query(env)
    assert end_stream(plain) is None
    assert plain.headers["delta-sharing-capabilities"] == "responseformat=parquet"
    asked = env["client"].post(f"{TABLE}/query", json={}, headers={
        "delta-sharing-capabilities": "responseformat=parquet;includeendstreamaction=true"})
    assert end_stream(asked) is not None
    assert asked.headers["delta-sharing-capabilities"] == "responseformat=parquet;includeendstreamaction=true"


@pytest.mark.parametrize("token", [
    "!!not-base64",
    base64.urlsafe_b64encode(b'{"o": 1}').decode(),
    base64.urlsafe_b64encode(b"[1, 2]").decode(),
    base64.urlsafe_b64encode(json.dumps({"o": 1, "m": f"{BETA_DIR}/v1.metadata.json"}).encode()).decode(),
    base64.urlsafe_b64encode(json.dumps({"o": -5, "m": f"{ALPHA_DIR}/v1.metadata.json"}).encode()).decode(),
    base64.urlsafe_b64encode(json.dumps({"o": 1, "m": f"{ALPHA_DIR}/v1-manifest.avro"}).encode()).decode(),
])
def test_rejects_bad_page_tokens(env, token):
    response = query(env, {"pageToken": token})
    assert response.status_code == 400
    assert response.json()["errorCode"] == "INVALID_PARAMETER_VALUE"


@pytest.mark.parametrize("token", [
    "!!not-base64",
    base64.urlsafe_b64encode(json.dumps({"m": f"{BETA_DIR}/v1.metadata.json"}).encode()).decode(),
])
def test_rejects_bad_refresh_tokens(env, token):
    response = query(env, {"refreshToken": token})
    assert response.status_code == 400
    assert response.json()["errorCode"] == "INVALID_PARAMETER_VALUE"


@pytest.mark.parametrize("body,status", [
    ({"version": 1_700_000_000_000}, 200),
    ({"version": 0}, 400),
    ({"version": "x"}, 400),
    ({"timestamp": "2030-01-01T00:00:00Z"}, 200),
    ({"timestamp": "2020-01-01T00:00:00Z"}, 400),
    ({"timestamp": "yesterday"}, 400),
    ({"startingVersion": 0}, 400),
    ({"maxFiles": "many"}, 400),
])
def test_only_the_current_snapshot_is_served(env, body, status):
    assert query(env, body).status_code == status


def test_version_endpoint_rejects_older_timestamps(env):
    assert env["client"].get(f"{TABLE}/version").headers["delta-table-version"] == "1700000000000"
    assert env["client"].get(f"{TABLE}/version", params={"startingTimestamp": "2020-01-01T00:00:00Z"}).status_code == 400


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]"])
def test_rejects_malformed_bodies(env, raw):
    assert query(env, raw=raw).status_code == 400


@pytest.mark.parametrize("url", [
    "/delta-sharing/shares/healthlake/schemas/alpha/tables/nope/metadata",
    "/delta-sharing/shares/healthlake/schemas/nope/tables/patient/metadata",
    "/delta-sharing/shares/nope/schemas",
])
def test_unknown_names_are_404(env, url):
    response = env["client"].get(url)
    assert response.status_code == 404
    assert response.json()["errorCode"] == "NOT_FOUND"


def test_presigned_urls_match_botocore(env, monkeypatch):
    import boto3
    import botocore.auth
    from botocore.config import Config
    monkeypatch.setattr(botocore.auth, "get_current_datetime", lambda *a, **k: datetime.datetime(2026, 10, 8, 12, 0, 0))
    creds = env["server"].Credentials("AKIAEXAMPLE", "secret", "session-token")
    key = "datalake/db/patient/data/00000-0 a+b=c~d.parquet"
    ours = env["server"].presign(creds, f"s3://alpha-bucket/{key}", 900)
    s3 = boto3.client("s3", region_name="us-east-1", endpoint_url="https://s3.us-east-1.amazonaws.com",
                      aws_access_key_id="AKIAEXAMPLE", aws_secret_access_key="secret", aws_session_token="session-token",
                      config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))
    theirs = s3.generate_presigned_url("get_object", Params={"Bucket": "alpha-bucket", "Key": key}, ExpiresIn=900)
    assert ours == theirs


def test_missing_s3_object_is_a_502(env):
    del env["objects"][f"{ALPHA_DIR}/v1-manifest.avro"]
    response = query(env)
    assert response.status_code == 502
    assert response.json()["errorCode"] == "AWS_ERROR"


def test_unexpected_errors_are_json_and_logged(env, monkeypatch, capsys):
    def broken(*_):
        raise KeyError("metadata_location")

    monkeypatch.setattr(env["server"], "metadata_location", broken)
    response = query(env)
    assert response.status_code == 500
    assert response.json()["errorCode"] == "INTERNAL_ERROR"
    assert f"POST {TABLE}/query 500" in capsys.readouterr().out


def test_tables_with_iceberg_delete_files_fail_loudly(env):
    env["glue"]["role-a"].databases[ALPHA_LINK]["tables"]["patient"] = add_snapshot(
        env["objects"], ALPHA_DIR, "v2", ["d.parquet"], 2, 1_800_000_000_000, delete_files=["d-deletes.parquet"])
    response = query(env)
    assert response.status_code == 400  # not 5xx, which clients would keep retrying
    assert response.json()["errorCode"] == "UNSUPPORTED_TABLE"


def test_snapshots_with_inline_manifests_fail_loudly(env):
    location = add_snapshot(env["objects"], ALPHA_DIR, "v2", ["d.parquet"], 2, 1_800_000_000_000)
    meta = json.loads(env["objects"][location])
    snap = meta["snapshots"][0]
    del snap["manifest-list"]
    snap["manifests"] = [f"{ALPHA_DIR}/v2-manifest.avro"]  # Iceberg v1 allows this instead of a manifest list
    env["objects"][location] = json.dumps(meta).encode()
    env["glue"]["role-a"].databases[ALPHA_LINK]["tables"]["patient"] = location
    response = query(env)
    assert response.status_code == 400
    assert response.json()["errorCode"] == "UNSUPPORTED_TABLE"


def test_store_without_grants_is_skipped_not_fatal(env):
    glue = env["glue"]["role-b"]
    original = glue.get_database

    def denied(Name):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "GetDatabase")

    glue.get_database = denied
    schemas = env["client"].get("/delta-sharing/shares/healthlake/schemas").json()["items"]
    assert [s["name"] for s in schemas] == [ALPHA]
    glue.get_database = original


def test_vending_refusal_is_reported(env):
    env["lf"]["role-a"].deny = True
    response = query(env)
    assert response.status_code == 403
    assert response.json()["errorCode"] == "LAKE_FORMATION_ACCESS_DENIED"
    assert ALPHA in response.json()["message"]
