"""
Global pytest fixtures for the Keboola DuckDB extension E2E test suite.

All live tests require KEBOOLA_TOKEN and KEBOOLA_URL environment variables.
Tests skip automatically when credentials are not provided — they never hard-fail.

Isolation strategy: every test receives a unique `test_prefix` derived from
uuid4 so that parallel runs and retries never collide. Teardown always runs
(try/finally inside yield fixtures) to avoid leaving orphaned resources.
"""

import os
import uuid
import time
import pytest
import requests
import duckdb


# ---------------------------------------------------------------------------
# pytest configuration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    config.addinivalue_line("markers", "live: requires live Keboola connection (KEBOOLA_TOKEN + KEBOOLA_URL)")
    config.addinivalue_line("markers", "offline: no network required, always runnable in CI")


# ---------------------------------------------------------------------------
# Credential / connection fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def keboola_token():
    """Read KEBOOLA_TOKEN env var; skip the entire session if missing."""
    token = os.environ.get("KEBOOLA_TOKEN")
    if not token:
        pytest.skip("KEBOOLA_TOKEN environment variable not set — skipping live tests")
    return token


@pytest.fixture(scope="session")
def keboola_url():
    """Read KEBOOLA_URL env var, defaulting to the US production stack."""
    return os.environ.get("KEBOOLA_URL", "https://connection.keboola.com").rstrip("/")


@pytest.fixture(scope="session")
def keboola_extension_path():
    """Path to the compiled .duckdb_extension file."""
    return os.environ.get(
        "KEBOOLA_EXTENSION_PATH",
        "../../build/release/extension/keboola/keboola.duckdb_extension",
    )


# ---------------------------------------------------------------------------
# DuckDB connection fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def duckdb_con(keboola_extension_path):
    """Fresh in-memory DuckDB connection per test with keboola extension loaded."""
    # allow_unsigned_extensions must be set at connection creation time (v1.5.0+)
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": True})
    con.execute(f"LOAD '{keboola_extension_path}';")
    yield con
    con.close()


@pytest.fixture
def kbc(duckdb_con, keboola_token, keboola_url):
    """
    Attaches Keboola as 'kbc' in the duckdb_con, yields the connection,
    and detaches in teardown even on test failure.
    """
    # READ_WRITE is required: DuckDB auto-sets HTTPS URLs to READ_ONLY by default.
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc (
            TYPE keboola,
            TOKEN '{keboola_token}',
            READ_WRITE
        )
    """)
    try:
        yield duckdb_con
    finally:
        try:
            duckdb_con.execute("DETACH kbc;")
        except Exception:
            pass  # already detached or connection closed


# ---------------------------------------------------------------------------
# Test isolation fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def test_prefix():
    """Unique prefix for all resources created in a single test run."""
    short_id = uuid.uuid4().hex[:8]
    return f"duckdb_test_{short_id}_"


# ---------------------------------------------------------------------------
# Keboola Storage API helper
# ---------------------------------------------------------------------------

class KeboolaStorageApi:
    """Thin wrapper around the Keboola Storage REST API used in fixtures."""

    def __init__(self, url: str, token: str):
        self.base_url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "X-StorageApi-Token": token,
        })

    def get(self, path: str, **kwargs):
        r = self.session.get(f"{self.base_url}/v2/storage{path}", **kwargs)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, data=None, json=None, **kwargs):
        r = self.session.post(f"{self.base_url}/v2/storage{path}", data=data, json=json, **kwargs)
        r.raise_for_status()
        return r.json()

    def delete(self, path: str, **kwargs):
        r = self.session.delete(f"{self.base_url}/v2/storage{path}", **kwargs)
        r.raise_for_status()
        return r

    def wait_for_job(self, job_id: str, timeout: int = 120, poll_interval: float = 1.0):
        """Poll a Storage API async job until it finishes or times out."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.get(f"/jobs/{job_id}")
            status = job.get("status")
            if status == "success":
                return job
            if status in ("error", "cancelled"):
                raise RuntimeError(f"Storage API job {job_id} failed: {job.get('error', {})}")
            time.sleep(poll_interval)
        raise TimeoutError(f"Storage API job {job_id} did not complete within {timeout}s")

    def create_bucket(self, stage: str, bucket_name: str, description: str = "") -> dict:
        """Create a bucket and return the bucket info dict."""
        return self.post("/buckets", data={
            "stage": stage,
            "name": bucket_name,
            "description": description,
        })

    def delete_bucket(self, bucket_id: str, force: bool = True):
        """Delete a bucket (optionally force-deleting its tables first)."""
        try:
            self.delete(f"/buckets/{bucket_id}", params={"force": int(force)})
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return  # already gone
            raise

    def create_table(self, bucket_id: str, table_name: str, columns: list[str],
                     primary_key: list[str] | None = None) -> dict:
        """Create a table via tables-definition endpoint and return table info."""
        payload = {
            "name": table_name,
            "columns": [{"name": c} for c in columns],
        }
        if primary_key:
            payload["primaryKeysNames"] = primary_key
        return self.post(f"/buckets/{bucket_id}/tables-definition", json=payload)

    def delete_table(self, table_id: str):
        """Delete a table, ignoring 404."""
        try:
            self.delete(f"/tables/{table_id}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return
            raise

    def list_workspaces(self) -> list[dict]:
        return self.get("/workspaces")

    def delete_workspace(self, workspace_id: str | int):
        try:
            self.delete(f"/workspaces/{workspace_id}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return
            raise


@pytest.fixture(scope="session")
def storage_api(keboola_token, keboola_url):
    """Session-scoped Storage API client."""
    return KeboolaStorageApi(keboola_url, keboola_token)


# ---------------------------------------------------------------------------
# Bucket fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def test_bucket(storage_api, test_prefix):
    """
    Creates `in.c-duckdbtest-{short_id}` bucket via the Storage API,
    yields the bucket id (e.g. 'in.c-duckdbtest-abc123'), and deletes
    it in teardown even if the test fails.
    """
    # Bucket names follow 'c-<name>' convention; Storage API prefixes stage
    bucket_name = f"c-duckdbtest-{test_prefix.replace('_', '-').rstrip('-')}"
    bucket_info = storage_api.create_bucket("in", bucket_name, description="DuckDB E2E test bucket")
    bucket_id = bucket_info["id"]  # e.g. "in.c-duckdbtest-..."
    try:
        yield bucket_id
    finally:
        storage_api.delete_bucket(bucket_id, force=True)


# ---------------------------------------------------------------------------
# Table fixture
# ---------------------------------------------------------------------------

STANDARD_COLUMNS = ["id", "name", "value"]

@pytest.fixture
def test_table(storage_api, test_bucket, test_prefix):
    """
    Creates a table with schema (id VARCHAR, name VARCHAR, value VARCHAR)
    in test_bucket.  Yields a dict with keys:
        bucket_id, table_id, table_name, columns
    Cleans up the table in teardown (bucket teardown also covers it, but
    this is explicit).
    """
    table_name = f"{test_prefix}contacts"
    table_info = storage_api.create_table(
        test_bucket,
        table_name,
        columns=STANDARD_COLUMNS,
        primary_key=["id"],
    )
    table_id = table_info["id"]  # e.g. "in.c-duckdbtest-....duckdb_test_xxxx_contacts"
    info = {
        "bucket_id": test_bucket,
        "table_id": table_id,
        "table_name": table_name,
        "columns": STANDARD_COLUMNS,
    }
    try:
        yield info
    finally:
        storage_api.delete_table(table_id)


# ---------------------------------------------------------------------------
# Typed table fixture (for type round-trip tests)
# ---------------------------------------------------------------------------

TYPED_COLUMNS = [
    {"name": "id",           "definition": {"type": "VARCHAR"}},
    {"name": "col_varchar",  "definition": {"type": "VARCHAR"}},
    {"name": "col_bigint",   "definition": {"type": "BIGINT"}},
    {"name": "col_double",   "definition": {"type": "DOUBLE"}},
    {"name": "col_boolean",  "definition": {"type": "BOOLEAN"}},
    {"name": "col_date",     "definition": {"type": "DATE"}},
    {"name": "col_timestamp","definition": {"type": "TIMESTAMP"}},
]

@pytest.fixture
def typed_test_table(storage_api, test_bucket, test_prefix):
    """
    Creates a table with strongly-typed columns for type round-trip tests.
    Yields same dict shape as test_table.
    """
    table_name = f"{test_prefix}typed"
    payload = {
        "name": table_name,
        "columns": TYPED_COLUMNS,
        "primaryKeysNames": ["id"],
    }
    import requests as req_lib
    base_url = storage_api.base_url
    r = storage_api.session.post(
        f"{base_url}/v2/storage/buckets/{test_bucket}/tables-definition",
        json=payload,
    )
    r.raise_for_status()
    table_info = r.json()
    table_id = table_info["id"]
    info = {
        "bucket_id": test_bucket,
        "table_id": table_id,
        "table_name": table_name,
        "columns": [c["name"] for c in TYPED_COLUMNS],
    }
    try:
        yield info
    finally:
        storage_api.delete_table(table_id)


# ---------------------------------------------------------------------------
# Utility: split table_id into (schema, table) for DuckDB SQL
# ---------------------------------------------------------------------------

def kbc_table_ref(table_id: str) -> str:
    """
    Convert a Storage API table id like 'in.c-foo.bar' to a DuckDB table
    reference 'kbc."in.c-foo".bar' suitable for use in SQL statements.
    """
    # table_id format: <stage>.<bucket_suffix>.<table_name>
    # e.g. in.c-duckdbtest-abc123ef.duckdb_test_abc123ef_contacts
    parts = table_id.split(".")
    if len(parts) < 3:
        raise ValueError(f"Unexpected table_id format: {table_id!r}")
    schema = ".".join(parts[:-1])
    table = parts[-1]
    return f'kbc."{schema}".{table}'
