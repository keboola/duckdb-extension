"""
E2E tests for SNAPSHOT mode.

SNAPSHOT mode pulls all Keboola data into local DuckDB on ATTACH.
After that:
- SELECTs run locally (no Snowflake / Query Service round-trip)
- Writes (INSERT/UPDATE/DELETE) still go to Keboola APIs
- keboola_pull() refreshes individual tables or all tables

Network isolation is tested by patching socket to verify no outbound
connections happen after the initial snapshot is loaded.
"""

import io
import csv
import socket
import pytest
import pandas as pd
import unittest.mock as mock
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------

def _upload_rows(storage_api, table_id: str, rows: list[dict], incremental: bool = False):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    index = storage_api.get("")
    importer_url = next(
        (svc["url"] for svc in index.get("services", []) if svc.get("id") == "import"),
        None,
    )
    if importer_url is None:
        pytest.skip("Importer service URL not available")

    r = storage_api.session.post(
        f"{importer_url}/write-table",
        data={"tableId": table_id, "incremental": "1" if incremental else "0",
              "delimiter": ",", "enclosure": '"'},
        files={"data": ("data.csv", buf.getvalue().encode(), "text/csv")},
    )
    r.raise_for_status()


SEED_ROWS = [
    {"id": "s1", "name": "SnapAlice",   "value": "snap_100"},
    {"id": "s2", "name": "SnapBob",     "value": "snap_200"},
    {"id": "s3", "name": "SnapCharlie", "value": "snap_300"},
]


# ---------------------------------------------------------------------------
# 1. SNAPSHOT loads data locally
# ---------------------------------------------------------------------------

def test_snapshot_loads_data(duckdb_con, keboola_token, keboola_url,
                             storage_api, test_table, test_prefix):
    """ATTACH SNAPSHOT followed by SELECT must return the seeded rows."""
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS)

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_snap_data (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_snap_data.", 1)
        df = duckdb_con.execute(f"SELECT * FROM {ref} ORDER BY id;").fetchdf()
        assert len(df) == len(SEED_ROWS), (
            f"Expected {len(SEED_ROWS)} rows in SNAPSHOT, got {len(df)}"
        )
        assert df.iloc[0]["name"] == "SnapAlice"
        assert df.iloc[2]["name"] == "SnapCharlie"
    finally:
        try:
            duckdb_con.execute("DETACH kbc_snap_data;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 2. SNAPSHOT is offline after load (no network calls during SELECT)
# ---------------------------------------------------------------------------

def test_snapshot_is_offline_after_load(duckdb_con, keboola_token, keboola_url,
                                        storage_api, test_table):
    """After snapshot load, SELECT queries must not make outbound network calls."""
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS)

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_snap_offline (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_snap_offline.", 1)

        # Warm-up: trigger lazy pull so data is local before we intercept sockets.
        duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};")

        # Track socket.create_connection calls after the snapshot is loaded
        original_connect = socket.create_connection
        connect_calls: list = []

        def mock_connect(address, *args, **kwargs):
            connect_calls.append(address)
            return original_connect(address, *args, **kwargs)

        with mock.patch("socket.create_connection", side_effect=mock_connect):
            df = duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()

        # We allow the mock to succeed (so we don't break the connection), but
        # no new connection attempt should be made for a purely local SELECT.
        keboola_host = keboola_url.replace("https://", "").replace("http://", "").split("/")[0]
        remote_calls = [addr for addr in connect_calls if keboola_host in str(addr)]
        assert len(remote_calls) == 0, (
            f"SNAPSHOT SELECT made {len(remote_calls)} unexpected network calls to {keboola_host}: "
            f"{remote_calls}"
        )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_snap_offline;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 3. keboola_pull() refreshes a single table
# ---------------------------------------------------------------------------

def test_keboola_pull_single_table(duckdb_con, keboola_token, keboola_url,
                                   storage_api, test_table):
    """keboola_pull() on a single table should reflect new data added after snapshot."""
    # Seed initial data
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS)

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_pull (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_pull.", 1)
        schema = ".".join(test_table["table_id"].split(".")[:-1])
        table_name = test_table["table_id"].split(".")[-1]

        count_before = duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
        assert count_before == len(SEED_ROWS)

        # Add a new row to Keboola after snapshot
        _upload_rows(storage_api, test_table["table_id"],
                     [{"id": "s99", "name": "NewRow", "value": "snap_999"}],
                     incremental=True)

        # Without pull, local snapshot still shows old count
        count_no_pull = duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
        assert count_no_pull == len(SEED_ROWS), (
            "Before keboola_pull(), local snapshot should still show old data"
        )

        # Pull single table
        duckdb_con.execute(f"CALL keboola_pull('kbc_pull.\"{schema}\".{table_name}');")

        # Now the new row should be visible
        count_after = duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
        assert count_after == len(SEED_ROWS) + 1, (
            f"After keboola_pull(), expected {len(SEED_ROWS) + 1} rows, got {count_after}"
        )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_pull;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 4. keboola_pull('kbc') refreshes all tables
# ---------------------------------------------------------------------------

def test_keboola_pull_all(duckdb_con, keboola_token, keboola_url,
                          storage_api, test_table):
    """CALL keboola_pull('kbc') should refresh all snapshot tables."""
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS)

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_pull_all (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_pull_all.", 1)

        # Add more data after snapshot
        _upload_rows(storage_api, test_table["table_id"],
                     [{"id": "sa1", "name": "AllPullRow", "value": "ap_val"}],
                     incremental=True)

        # Pull all
        duckdb_con.execute("CALL keboola_pull('kbc_pull_all');")

        count = duckdb_con.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
        assert count >= len(SEED_ROWS) + 1, (
            f"After keboola_pull('kbc_pull_all'), expected at least {len(SEED_ROWS) + 1} rows, got {count}"
        )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_pull_all;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 5. SNAPSHOT write still goes to Keboola
# ---------------------------------------------------------------------------

def test_snapshot_write_still_live(duckdb_con, keboola_token, keboola_url,
                                   storage_api, test_table):
    """
    In SNAPSHOT mode, INSERT should still persist data to Keboola.
    After INSERT, keboola_pull() must show the new row.
    """
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS)

    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_snap_write (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_snap_write.", 1)
        schema = ".".join(test_table["table_id"].split(".")[:-1])
        table_name = test_table["table_id"].split(".")[-1]

        # Insert via DuckDB — should go to Keboola
        duckdb_con.execute(f"INSERT INTO {ref} VALUES ('sw1', 'SnapWrite', 'live_write');")

        # Pull to refresh local snapshot from Keboola
        duckdb_con.execute(f"CALL keboola_pull('kbc_snap_write.\"{schema}\".{table_name}');")

        # Now the inserted row should be visible
        df = duckdb_con.execute(f"SELECT id FROM {ref} WHERE id = 'sw1';").fetchdf()
        assert len(df) == 1, (
            "Inserted row 'sw1' not found after keboola_pull() in SNAPSHOT mode. "
            "INSERT may not have been persisted to Keboola."
        )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_snap_write;")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 6. keboola_pull() with wrong table name raises error
# ---------------------------------------------------------------------------

def test_keboola_pull_wrong_table_fails(duckdb_con, keboola_token, keboola_url):
    """keboola_pull() with an invalid table reference must raise a descriptive error."""
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_snap_err (
            TYPE keboola,
            TOKEN '{keboola_token}',
            SNAPSHOT
        )
    """)
    try:
        with pytest.raises(Exception) as exc_info:
            duckdb_con.execute(
                'CALL keboola_pull(\'kbc_snap_err."in.c-does-not-exist-xyz".no_table\');'
            )
        error_msg = str(exc_info.value).lower()
        # Must give a meaningful message, not a segfault or empty error
        assert any(kw in error_msg for kw in ("not found", "does not exist", "error", "invalid")), (
            f"Expected descriptive error, got: {exc_info.value}"
        )
    finally:
        try:
            duckdb_con.execute("DETACH kbc_snap_err;")
        except Exception:
            pass
