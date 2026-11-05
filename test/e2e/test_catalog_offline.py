"""
Offline catalog tests against a local fake Storage API.

The fake serves the endpoints ATTACH reads (index, branches, workspaces, the
legacy table list and table list V2) from canned JSON, so these tests need no
credentials. They cover how the catalog builds column types and descriptions:

- table and column descriptions from the native definition surface as DuckDB
  comments (duckdb_tables().comment, information_schema.columns.column_comment);
- a typed table whose definition table list V2 leaves null takes its column
  types from the `storage` KBC.datatype.* rows of the legacy table list (the
  root source table's rows for an alias), without a per-table detail request;
- such a table never fails ATTACH, also when it has no type rows at all.
"""

import copy
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

pytestmark = pytest.mark.offline

BUCKET_ID = "in.c-crm"
ATTACH_NAME = "kbc_offline"


def _row(key, value, provider="storage"):
    return {"id": "1", "key": key, "value": value, "provider": provider, "timestamp": "2026-11-01T00:00:00+0000"}


def _legacy_table(name, columns, column_metadata, is_typed=True, source_column_metadata=None):
    table = {
        "id": f"{BUCKET_ID}.{name}",
        "name": name,
        "isTyped": is_typed,
        "primaryKey": [],
        "columns": columns,
        "columnMetadata": column_metadata,
    }
    if source_column_metadata is not None:
        table["sourceTable"] = {"id": "in.c-root.orders", "columnMetadata": source_column_metadata}
    return table


def _v2_item(name, description, definition):
    return {
        "id": f"{BUCKET_ID}.{name}",
        "bucketId": BUCKET_ID,
        "name": name,
        "description": description,
        "definition": definition,
    }


# Typed table with a native definition. Its stale `storage` rows say VARCHAR;
# the definition wins.
NATIVE_TYPED = _legacy_table(
    "native_typed",
    ["id", "name"],
    {
        "id": [_row("KBC.datatype.type", "VARCHAR"), _row("KBC.description", "stale metadata text")],
        "name": [_row("KBC.datatype.type", "VARCHAR")],
    },
)
NATIVE_TYPED_V2 = _v2_item(
    "native_typed",
    "Native table description",
    {
        "description": "Native table description",
        "primaryKeysNames": [],
        "columns": [
            {"name": "id", "basetype": "NUMERIC",
             "definition": {"type": "NUMBER", "length": "38,0", "nullable": False, "description": "Native id description"}},
            {"name": "name", "basetype": "STRING", "definition": {"type": "VARCHAR", "nullable": True}},
        ],
    },
)

# Typed table whose column types are not all native: table list V2 leaves the
# definition null. Types come from the `storage` rows; rows of other providers
# and KBC.description rows are not read.
KV_TYPED = _legacy_table(
    "kv_typed",
    ["amount", "created", "note"],
    {
        "amount": [
            _row("KBC.datatype.type", "NUMBER"),
            _row("KBC.datatype.length", "12,2"),
            _row("KBC.datatype.nullable", ""),
            _row("KBC.description", "storage row description"),
        ],
        "created": [_row("KBC.datatype.type", "TIMESTAMP_NTZ"), _row("KBC.datatype.basetype", "TIMESTAMP")],
        "note": [_row("KBC.datatype.type", "BIGINT", provider="keboola.ex-db-snowflake")],
    },
)
KV_TYPED_V2 = _v2_item("kv_typed", "KV table description", None)

# Typed table with a null definition and no type rows at all.
BARE_TYPED = _legacy_table("bare_typed", ["a", "b"], {})
BARE_TYPED_V2 = _v2_item("bare_typed", "", None)

# Alias of a null-definition typed table: its own column metadata is empty,
# the root's `storage` rows type it.
ALIAS_TYPED = _legacy_table(
    "alias_typed",
    ["qty"],
    {},
    source_column_metadata={"qty": [_row("KBC.datatype.type", "INTEGER")]},
)
ALIAS_TYPED_V2 = _v2_item("alias_typed", "", None)

# Untyped table: types from KBC.datatype.* rows of any provider, the column
# description from the native definition.
UNTYPED = _legacy_table(
    "untyped",
    ["flag"],
    {"flag": [_row("KBC.datatype.basetype", "BOOLEAN", provider="keboola.ex-db-snowflake")]},
    is_typed=False,
)
UNTYPED_V2 = _v2_item(
    "untyped",
    "",
    {
        "description": "",
        "primaryKeysNames": [],
        "columns": [{"name": "flag", "definition": {"description": "Untyped flag description"}}],
    },
)

# Alias created after ATTACH: only the per-bucket refresh (FetchBucketTables)
# sees it.
LATE_ALIAS = _legacy_table(
    "late_alias",
    ["total"],
    {},
    source_column_metadata={"total": [_row("KBC.datatype.type", "NUMBER"), _row("KBC.datatype.length", "10,3")]},
)
LATE_ALIAS_V2 = _v2_item("late_alias", "Late alias description", None)

LEGACY_TABLES = [NATIVE_TYPED, KV_TYPED, BARE_TYPED, ALIAS_TYPED, UNTYPED]
# Two V2 pages, linked through an absolute `links.next` with a JSON:API cursor.
V2_PAGES = [
    [NATIVE_TYPED_V2, KV_TYPED_V2, BARE_TYPED_V2],
    [ALIAS_TYPED_V2, UNTYPED_V2],
]


class FakeStorageApi(BaseHTTPRequestHandler):
    """Serves the canned project; every request lands in `server.requests`."""

    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler signature
        pass

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _base_url(self):
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    @staticmethod
    def _legacy_list(tables, query):
        # The roots of these aliases sit in another project (a linked bucket):
        # the server emits their column metadata only for sourceColumnMetadata.
        includes = ",".join(query.get("include", [])).split(",")
        result = copy.deepcopy(tables)
        if "sourceColumnMetadata" not in includes:
            for table in result:
                if "sourceTable" in table:
                    table["sourceTable"]["columnMetadata"] = []
        return result

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler naming
        self.server.requests.append(("GET", self.path))
        url = urlsplit(self.path)
        path = url.path
        query = parse_qs(unquote(url.query))

        if path == "/v2/storage":
            self._send(200, {"url": self._base_url(), "services": [{"id": "query", "url": self._base_url() + "/query"}]})
        elif path == "/v2/storage/dev-branches/":
            self._send(200, [{"id": 1, "name": "Main", "isDefault": True}])
        elif path == "/v2/storage/tokens/verify":
            self._send(200, {"owner": {"defaultBackend": "snowflake"}})
        elif path == "/v2/storage/workspaces":
            self._send(200, [])
        elif path == "/v2/storage/buckets":
            self._send(200, [{"id": BUCKET_ID, "name": "c-crm", "stage": "in", "description": "",
                              "isLinkedBucket": False, "backendPath": ["DB", BUCKET_ID]}])
        elif path in ("/v2/storage/tables", f"/v2/storage/buckets/{BUCKET_ID}/tables"):
            tables = LEGACY_TABLES if path == "/v2/storage/tables" else LEGACY_TABLES + [LATE_ALIAS]
            self._send(200, self._legacy_list(tables, query))
        elif path == "/v2/storage/branch/default/table-list" and "filter[bucketId]" in query:
            items = [item for page in V2_PAGES for item in page] + [LATE_ALIAS_V2]
            self._send(200, {"data": items, "links": {"next": None}})
        elif path == "/v2/storage/branch/default/table-list":
            page = int(query.get("page[after]", ["0"])[0])
            links = {"next": None}
            if page + 1 < len(V2_PAGES):
                links["next"] = f"{self._base_url()}/v2/storage/branch/default/table-list?include=definition&page[after]={page + 1}"
            self._send(200, {"data": V2_PAGES[page], "links": links})
        elif path.startswith("/v2/storage/tables/"):
            # The catalog must not need the table detail; answer it with a
            # server error so a request to it fails ATTACH.
            self._send(500, {"error": "table detail is not served by the offline fake"})
        else:
            self._send(404, {"error": f"not served: {path}"})

    def do_POST(self):  # noqa: N802
        self.server.requests.append(("POST", self.path))
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        if urlsplit(self.path).path == "/v2/storage/workspaces":
            self._send(200, {"id": 4242, "name": "offline", "connection": {"backend": "snowflake"}})
        else:
            self._send(404, {"error": f"not served: {self.path}"})

    def do_DELETE(self):  # noqa: N802
        self.server.requests.append(("DELETE", self.path))
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()


@pytest.fixture(scope="module")
def fake_storage_api():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeStorageApi)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="module")
def offline_kbc(duckdb_con, fake_storage_api):
    host, port = fake_storage_api.server_address[:2]
    duckdb_con.execute(
        f"ATTACH '' AS {ATTACH_NAME} (TYPE keboola, TOKEN 'offline-token', URL 'http://{host}:{port}')"
    )
    try:
        yield duckdb_con
    finally:
        try:
            duckdb_con.execute(f"DETACH {ATTACH_NAME};")
        except Exception:
            pass  # already detached or connection closed


def _columns(con, table_name):
    rows = con.execute(
        """
        SELECT column_name, data_type, column_comment
        FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ATTACH_NAME, BUCKET_ID, table_name],
    ).fetchall()
    return {name: (data_type, comment) for name, data_type, comment in rows}


def _table_comment(con, table_name):
    rows = con.execute(
        "SELECT comment FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [ATTACH_NAME, BUCKET_ID, table_name],
    ).fetchall()
    assert len(rows) == 1, f"Expected one duckdb_tables() row for {table_name}, got: {rows}"
    return rows[0][0]


def test_native_descriptions_are_comments(offline_kbc):
    assert _table_comment(offline_kbc, "native_typed") == "Native table description"
    columns = _columns(offline_kbc, "native_typed")
    assert columns["id"] == ("BIGINT", "Native id description")
    assert columns["name"] == ("VARCHAR", None)


def test_untyped_table_column_description_is_comment(offline_kbc):
    columns = _columns(offline_kbc, "untyped")
    assert columns["flag"] == ("BOOLEAN", "Untyped flag description")
    assert _table_comment(offline_kbc, "untyped") is None


def test_null_definition_typed_table_reads_storage_type_rows(offline_kbc):
    columns = _columns(offline_kbc, "kv_typed")
    assert columns["amount"] == ("DECIMAL(12,2)", None)
    assert columns["created"] == ("TIMESTAMP", None)
    # Only a non-storage provider types `note`: it is not read for a typed table.
    assert columns["note"] == ("VARCHAR", None)
    assert _table_comment(offline_kbc, "kv_typed") == "KV table description"


def test_null_definition_alias_reads_root_storage_type_rows(offline_kbc):
    columns = _columns(offline_kbc, "alias_typed")
    assert columns["qty"] == ("BIGINT", None)


def test_null_definition_typed_table_without_type_rows_attaches(offline_kbc):
    columns = _columns(offline_kbc, "bare_typed")
    assert columns["a"] == ("VARCHAR", None)
    assert columns["b"] == ("VARCHAR", None)


def test_attach_reads_no_table_detail(offline_kbc, fake_storage_api):
    detail_requests = [
        path for method, path in fake_storage_api.requests
        if method == "GET" and urlsplit(path).path.startswith("/v2/storage/tables/")
    ]
    assert detail_requests == []
    v2_requests = [
        path for method, path in fake_storage_api.requests
        if method == "GET" and urlsplit(path).path == "/v2/storage/branch/default/table-list"
    ]
    assert len(v2_requests) >= 2, f"Expected both table list V2 pages to be read, got: {v2_requests}"


def test_refresh_types_new_alias_from_root_storage_rows(offline_kbc, fake_storage_api):
    # Past the 2 s refresh throttle, a catalog scan re-reads the bucket.
    time.sleep(2.5)
    columns = _columns(offline_kbc, "late_alias")
    assert columns["total"] == ("DECIMAL(10,3)", None)
    assert _table_comment(offline_kbc, "late_alias") == "Late alias description"

    bucket_v2_requests = [
        path for method, path in fake_storage_api.requests
        if method == "GET"
        and urlsplit(path).path == "/v2/storage/branch/default/table-list"
        and "filter[bucketId]" in parse_qs(unquote(urlsplit(path).query))
    ]
    assert bucket_v2_requests, "Expected the refresh to read table list V2 filtered to the bucket"
    detail_requests = [
        path for method, path in fake_storage_api.requests
        if method == "GET" and urlsplit(path).path.startswith("/v2/storage/tables/")
    ]
    assert detail_requests == []
