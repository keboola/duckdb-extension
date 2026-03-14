"""
E2E tests for UPDATE operations via the Keboola DuckDB extension.

UPDATE is implemented as: fetch matching rows via Query Service →
merge SET values → re-upload via Storage Importer with incremental=1 →
Storage deduplicates on primary key.

Precondition: all test tables must have a PRIMARY KEY (`id` column).
"""

import io
import csv
import pytest
import pandas as pd
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------

def _upload_rows(storage_api, table_id: str, rows: list[dict]):
    """Upload rows to a table via the Storage Importer."""
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
        data={"tableId": table_id, "incremental": "0", "delimiter": ",", "enclosure": '"'},
        files={"data": ("data.csv", buf.getvalue().encode(), "text/csv")},
    )
    r.raise_for_status()


INITIAL_ROWS = [
    {"id": "1", "name": "Alice",   "value": "100"},
    {"id": "2", "name": "Bob",     "value": "200"},
    {"id": "3", "name": "Charlie", "value": "300"},
    {"id": "4", "name": "Diana",   "value": "400"},
    {"id": "5", "name": "Eve",     "value": "500"},
]


@pytest.fixture
def seeded_table(storage_api, test_table, kbc):
    """Upload INITIAL_ROWS and yield table info."""
    _upload_rows(storage_api, test_table["table_id"], INITIAL_ROWS)
    yield test_table


def _fetch_row(kbc_con, ref: str, row_id: str) -> pd.Series:
    df = kbc_con.execute(f"SELECT * FROM {ref} WHERE id = '{row_id}';").fetchdf()
    assert len(df) == 1, f"Expected exactly 1 row for id='{row_id}', got {len(df)}"
    return df.iloc[0]


# ---------------------------------------------------------------------------
# 1. Update single row
# ---------------------------------------------------------------------------

def test_update_single_row(kbc, seeded_table):
    """UPDATE one row by PK, verify the change is reflected."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"UPDATE {ref} SET name = 'AliceUpdated' WHERE id = '1';")
    row = _fetch_row(kbc, ref, "1")
    assert row["name"] == "AliceUpdated", f"Expected 'AliceUpdated', got {row['name']!r}"
    # Other fields must be unchanged
    assert row["value"] == "100"


# ---------------------------------------------------------------------------
# 2. Update multiple columns in one statement
# ---------------------------------------------------------------------------

def test_update_multiple_columns(kbc, seeded_table):
    """SET both name and value in one UPDATE."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"UPDATE {ref} SET name = 'BobRenamed', value = '999' WHERE id = '2';")
    row = _fetch_row(kbc, ref, "2")
    assert row["name"] == "BobRenamed"
    assert row["value"] == "999"


# ---------------------------------------------------------------------------
# 3. Update multiple rows (batch)
# ---------------------------------------------------------------------------

def test_update_multi_row_batch(kbc, seeded_table):
    """UPDATE WHERE id IN (...) must update all matching rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"UPDATE {ref} SET value = '0' WHERE id IN ('3', '4', '5');")

    for row_id in ("3", "4", "5"):
        row = _fetch_row(kbc, ref, row_id)
        assert row["value"] == "0", (
            f"Expected value='0' for id={row_id!r}, got {row['value']!r}"
        )
    # Rows 1 and 2 must be unaffected
    assert _fetch_row(kbc, ref, "1")["value"] == "100"
    assert _fetch_row(kbc, ref, "2")["value"] == "200"


# ---------------------------------------------------------------------------
# 4. SET column to NULL
# ---------------------------------------------------------------------------

def test_update_set_null(kbc, seeded_table):
    """UPDATE SET value = NULL; SELECT must return NULL (not empty string)."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"UPDATE {ref} SET value = NULL WHERE id = '1';")
    df = kbc.execute(f"SELECT value FROM {ref} WHERE id = '1';").fetchdf()
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["value"]), (
        f"Expected NULL after UPDATE SET NULL, got {df.iloc[0]['value']!r}"
    )


# ---------------------------------------------------------------------------
# 5. Update non-existent row (no rows matched)
# ---------------------------------------------------------------------------

def test_update_nonexistent_row(kbc, seeded_table):
    """UPDATE a row that doesn't exist should not raise an error but change 0 rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    # Should complete without exception
    kbc.execute(f"UPDATE {ref} SET name = 'Ghost' WHERE id = 'nonexistent_id_xyz';")
    # All original rows are still unchanged
    df = kbc.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()
    assert df[0] == len(INITIAL_ROWS)


# ---------------------------------------------------------------------------
# 6. Table without PK raises descriptive error
# ---------------------------------------------------------------------------

def test_update_no_pk_fails(test_bucket, kbc, storage_api, test_prefix):
    """UPDATE on a table without a primary key must raise a descriptive error."""
    # Create a table without PK
    table_name = f"{test_prefix}nopk"
    table_info = storage_api.create_table(
        test_bucket,
        table_name,
        columns=["id", "name", "value"],
        primary_key=None,   # explicitly no PK
    )
    table_id = table_info["id"]
    try:
        ref = kbc_table_ref(table_id)
        with pytest.raises(Exception) as exc_info:
            kbc.execute(f"UPDATE {ref} SET name = 'x' WHERE id = '1';")
        error_msg = str(exc_info.value).lower()
        assert any(kw in error_msg for kw in ("primary key", "pk", "not supported")), (
            f"Expected 'primary key' in error, got: {exc_info.value}"
        )
    finally:
        storage_api.delete_table(table_id)


# ---------------------------------------------------------------------------
# 7. WHERE on non-PK column raises error
# ---------------------------------------------------------------------------

def test_update_non_pk_where_fails(kbc, seeded_table):
    """UPDATE WHERE on a non-PK column must raise a NOT SUPPORTED error."""
    ref = kbc_table_ref(seeded_table["table_id"])
    with pytest.raises(Exception) as exc_info:
        kbc.execute(f"UPDATE {ref} SET value = '999' WHERE name = 'Alice';")
    error_msg = str(exc_info.value).lower()
    assert any(kw in error_msg for kw in ("primary key", "pk", "not supported", "where")), (
        f"Expected error about non-PK WHERE clause, got: {exc_info.value}"
    )


# ---------------------------------------------------------------------------
# 8. UPDATE on READ_ONLY attachment fails
# ---------------------------------------------------------------------------

def test_update_readonly_fails(duckdb_con, keboola_token, keboola_url, seeded_table):
    """UPDATE on a READ_ONLY-attached database must raise an error."""
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_ro_upd (
            TYPE keboola,
            TOKEN '{keboola_token}',
            READ_ONLY
        )
    """)
    try:
        ref = kbc_table_ref(seeded_table["table_id"]).replace("kbc.", "kbc_ro_upd.", 1)
        with pytest.raises(Exception, match=r"(?i)read.?only"):
            duckdb_con.execute(f"UPDATE {ref} SET name = 'x' WHERE id = '1';")
    finally:
        try:
            duckdb_con.execute("DETACH kbc_ro_upd;")
        except Exception:
            pass
