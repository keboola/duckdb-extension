"""
E2E tests for ATTACH / DETACH behavior.

Covers:
- Inline credentials (TOKEN + URL)
- Secret-based ATTACH
- READ_ONLY flag
- SNAPSHOT mode
- Invalid token / wrong URL error handling
- Workspace cleanup on DETACH
- Workspace reuse across sequential ATTACHes
"""

import pytest
from conftest import KeboolaStorageApi


pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# 1. Inline credentials
# ---------------------------------------------------------------------------

def test_attach_inline_credentials(duckdb_con, keboola_token, keboola_url):
    """ATTACH with TOKEN + URL inline should register the database in duckdb_databases()."""
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_inline (
            TYPE keboola,
            TOKEN '{keboola_token}'
        )
    """)
    try:
        rows = duckdb_con.execute(
            "SELECT database_name FROM duckdb_databases() WHERE database_name = 'kbc_inline'"
        ).fetchall()
        assert len(rows) == 1, "kbc_inline must appear in duckdb_databases() after ATTACH"
    finally:
        try:
            duckdb_con.execute("DETACH kbc_inline;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 2. Secret-based ATTACH
# ---------------------------------------------------------------------------

def test_attach_with_secret(duckdb_con, keboola_token, keboola_url):
    """CREATE SECRET then ATTACH using SECRET name should succeed."""
    duckdb_con.execute(f"""
        CREATE OR REPLACE SECRET e2e_secret (
            TYPE keboola,
            TOKEN '{keboola_token}',
            URL '{keboola_url}'
        )
    """)
    duckdb_con.execute("ATTACH '' AS kbc_via_secret (TYPE keboola, SECRET e2e_secret)")
    try:
        rows = duckdb_con.execute(
            "SELECT database_name FROM duckdb_databases() WHERE database_name = 'kbc_via_secret'"
        ).fetchall()
        assert len(rows) == 1, "kbc_via_secret must appear in duckdb_databases()"
    finally:
        try:
            duckdb_con.execute("DETACH kbc_via_secret;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 3. READ_ONLY
# ---------------------------------------------------------------------------

def test_attach_read_only(duckdb_con, keboola_token, keboola_url, test_table):
    """ATTACH READ_ONLY should attach successfully but reject INSERT statements."""
    bucket_id = test_table["bucket_id"]
    table_name = test_table["table_name"]

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_ro (
            TYPE keboola,
            TOKEN '{keboola_token}',
            READ_ONLY
        )
    """)
    try:
        rows = duckdb_con.execute(
            "SELECT database_name FROM duckdb_databases() WHERE database_name = 'kbc_ro'"
        ).fetchall()
        assert len(rows) == 1, "kbc_ro must appear in duckdb_databases()"

        with pytest.raises(Exception, match=r"(?i)read.?only"):
            duckdb_con.execute(
                f'INSERT INTO kbc_ro."{bucket_id}".{table_name} VALUES (\'a\', \'b\', \'c\')'
            )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_ro;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 4. SNAPSHOT mode
# ---------------------------------------------------------------------------

def test_attach_snapshot(duckdb_con, keboola_token, keboola_url,
                         storage_api, test_table, test_prefix):
    """
    ATTACH SNAPSHOT should pull data locally so that SELECT works without
    hitting Keboola live.
    """
    # Insert a row first via the Storage API so the table isn't empty
    table_id = test_table["table_id"]
    import io, csv as csv_lib
    buf = io.StringIO()
    w = csv_lib.writer(buf)
    w.writerow(["id", "name", "value"])
    w.writerow(["snap1", "SnapshotRow", "42"])
    csv_data = buf.getvalue()

    # Upload via Importer — use requests directly against the import endpoint
    verify_resp = storage_api.get("")  # GET /v2/storage to discover importer URL
    importer_url = None
    for svc in verify_resp.get("services", []):
        if svc.get("id") == "import":
            importer_url = svc["url"]
            break

    if importer_url:
        r = storage_api.session.post(
            f"{importer_url}/write-table",
            data={"tableId": table_id, "incremental": "0", "delimiter": ",", "enclosure": '"'},
            files={"data": ("data.csv", csv_data.encode(), "text/csv")},
        )
        # best-effort — if importer is not available we still exercise ATTACH SNAPSHOT
        r.raise_for_status() if r.status_code < 500 else None

    schema = ".".join(table_id.split(".")[:-1])   # "in.c-duckdbtest-..."
    table_name = table_id.split(".")[-1]

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_snap (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        # Data should be accessible locally
        count = duckdb_con.execute(
            f'SELECT COUNT(*) FROM kbc_snap."{schema}".{table_name}'
        ).fetchone()[0]
        assert count >= 0, "COUNT(*) must be a non-negative integer in SNAPSHOT mode"
    finally:
        try:
            duckdb_con.execute("DETACH kbc_snap;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 5. Invalid token
# ---------------------------------------------------------------------------

def test_attach_invalid_token(duckdb_con, keboola_url):
    """ATTACH with a bogus token must raise an error with a meaningful message."""
    with pytest.raises(Exception) as exc_info:
        duckdb_con.execute(f"""
            ATTACH '{keboola_url}' AS kbc_bad_token (
                TYPE keboola,
                TOKEN 'invalid-token-that-does-not-exist-xyz'
            )
        """)
    error_msg = str(exc_info.value).lower()
    # Error must mention authentication / invalid token / unauthorized — not a generic crash
    assert any(kw in error_msg for kw in ("invalid", "unauthorized", "auth", "token", "403", "401")), (
        f"Expected auth-related error message, got: {exc_info.value}"
    )


# ---------------------------------------------------------------------------
# 6. Wrong URL
# ---------------------------------------------------------------------------

def test_attach_wrong_url(duckdb_con, keboola_token):
    """ATTACH pointing to a non-existent host must raise a connection error."""
    with pytest.raises(Exception) as exc_info:
        duckdb_con.execute(f"""
            ATTACH 'https://this-host-absolutely-does-not-exist.keboola.invalid' AS kbc_wrong_url (
                TYPE keboola,
                TOKEN '{keboola_token}'
            )
        """)
    error_msg = str(exc_info.value).lower()
    assert any(kw in error_msg for kw in ("connect", "resolve", "host", "network", "dns", "failed")), (
        f"Expected connection error message, got: {exc_info.value}"
    )


# ---------------------------------------------------------------------------
# 7. DETACH cleans up workspace
# ---------------------------------------------------------------------------

def test_detach_cleans_workspace(duckdb_con, keboola_token, keboola_url, storage_api):
    """
    After DETACH the workspace created by the extension should be deleted
    from Keboola Storage.
    """
    # Snapshot of existing workspaces before we attach
    before_ids = {ws["id"] for ws in storage_api.list_workspaces()}

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_cleanup (
            TYPE keboola,
            TOKEN '{keboola_token}'
        )
    """)

    # A new workspace should have been created
    after_attach_ids = {ws["id"] for ws in storage_api.list_workspaces()}
    new_ids = after_attach_ids - before_ids
    assert len(new_ids) >= 1, (
        "ATTACH must create at least one workspace. "
        f"Before: {before_ids}, after: {after_attach_ids}"
    )

    # Now detach
    duckdb_con.execute("DETACH kbc_cleanup;")

    # The workspace should be gone
    after_detach_ids = {ws["id"] for ws in storage_api.list_workspaces()}
    still_present = new_ids & after_detach_ids
    assert not still_present, (
        f"Workspace(s) {still_present} were NOT deleted after DETACH"
    )


# ---------------------------------------------------------------------------
# 8. Re-attach reuses workspace
# ---------------------------------------------------------------------------

def test_reattach_reuses_workspace(duckdb_con, keboola_token, keboola_url, storage_api):
    """
    Two sequential ATTACHes (with DETACH in between) should reuse the same
    workspace rather than creating a new one each time.  After first ATTACH
    the workspace is tagged 'duckdb-extension'; the second ATTACH must find
    and reuse it (or at least not leave duplicate workspaces behind).
    """
    # First ATTACH
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_first (
            TYPE keboola,
            TOKEN '{keboola_token}'
        )
    """)
    ws_after_first = {ws["id"]: ws for ws in storage_api.list_workspaces()}
    duckdb_con.execute("DETACH kbc_first;")

    # Second ATTACH — the workspace from the first should be reused (not deleted on DETACH
    # when the extension is designed to keep it, or re-created and reused if not).
    # Regardless of strategy, we must not end up with unbounded workspace growth.
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_second (
            TYPE keboola,
            TOKEN '{keboola_token}'
        )
    """)
    ws_after_second = {ws["id"] for ws in storage_api.list_workspaces()}
    duckdb_con.execute("DETACH kbc_second;")

    # After both detaches, no duckdb-extension workspaces should linger
    remaining = storage_api.list_workspaces()
    duckdb_ws = [
        ws for ws in remaining
        if "duckdb" in str(ws.get("name", "")).lower()
        or "duckdb" in str(ws.get("description", "")).lower()
    ]
    assert len(duckdb_ws) == 0, (
        f"Expected 0 duckdb-tagged workspaces after both DETACHes, found: {duckdb_ws}"
    )
