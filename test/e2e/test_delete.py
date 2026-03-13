"""
E2E tests for DELETE operations via the Keboola DuckDB extension.

DELETE translates to Storage API: DELETE /v2/storage/tables/{tableId}/rows
with deleteWhereColumn / deleteWhereValues[] / deleteWhereOperator params.

Phase-1 limitation: single-column WHERE only (eq, IN, ne).
A plain DELETE without WHERE is guarded (raises error) to prevent accidental truncation.
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
    {"id": "1", "name": "Alice",   "value": "keep"},
    {"id": "2", "name": "Bob",     "value": "delete_me"},
    {"id": "3", "name": "Charlie", "value": "delete_me"},
    {"id": "4", "name": "Diana",   "value": "keep"},
    {"id": "5", "name": "Eve",     "value": "keep"},
]


@pytest.fixture
def seeded_table(storage_api, test_table, kbc):
    _upload_rows(storage_api, test_table["table_id"], INITIAL_ROWS)
    yield test_table


def _ids_present(kbc_con, ref: str) -> set:
    df = kbc_con.execute(f"SELECT id FROM {ref};").fetchdf()
    return set(df["id"].tolist())


# ---------------------------------------------------------------------------
# 1. DELETE single value (equality)
# ---------------------------------------------------------------------------

def test_delete_single_value(kbc, seeded_table):
    """DELETE WHERE id = '2' must remove exactly that row."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"DELETE FROM {ref} WHERE id = '2';")
    ids = _ids_present(kbc, ref)
    assert "2" not in ids, "Row with id='2' should have been deleted"
    # All other rows must remain
    for row_id in ("1", "3", "4", "5"):
        assert row_id in ids, f"Row {row_id} should NOT have been deleted"


# ---------------------------------------------------------------------------
# 2. DELETE IN list
# ---------------------------------------------------------------------------

def test_delete_in_list(kbc, seeded_table):
    """DELETE WHERE id IN ('2','3') must remove both rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"DELETE FROM {ref} WHERE id IN ('2', '3');")
    ids = _ids_present(kbc, ref)
    assert "2" not in ids, "Row id='2' should be deleted"
    assert "3" not in ids, "Row id='3' should be deleted"
    assert len(ids) == 3, f"Expected 3 remaining rows, got {len(ids)}: {ids}"


# ---------------------------------------------------------------------------
# 3. DELETE NOT EQUAL
# ---------------------------------------------------------------------------

def test_delete_not_equal(kbc, seeded_table):
    """DELETE WHERE value != 'keep' must delete rows 2 and 3, leaving 1, 4, 5."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"DELETE FROM {ref} WHERE value != 'keep';")
    ids = _ids_present(kbc, ref)
    assert ids == {"1", "4", "5"}, (
        f"Expected ids {{1, 4, 5}} to remain, got {ids}"
    )


# ---------------------------------------------------------------------------
# 4. DELETE non-existent row
# ---------------------------------------------------------------------------

def test_delete_nonexistent_row(kbc, seeded_table):
    """DELETE WHERE id = 'ghost' must complete without error; row count unchanged."""
    ref = kbc_table_ref(seeded_table["table_id"])
    kbc.execute(f"DELETE FROM {ref} WHERE id = 'ghost_id_xyz_does_not_exist';")
    count = kbc.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
    assert count == len(INITIAL_ROWS), (
        f"Expected {len(INITIAL_ROWS)} rows (unchanged), got {count}"
    )


# ---------------------------------------------------------------------------
# 5. DELETE without WHERE must raise (truncate guard)
# ---------------------------------------------------------------------------

def test_delete_without_where_fails(kbc, seeded_table):
    """Plain DELETE (no WHERE clause) must raise a truncate-guard error."""
    ref = kbc_table_ref(seeded_table["table_id"])
    with pytest.raises(Exception) as exc_info:
        kbc.execute(f"DELETE FROM {ref};")
    error_msg = str(exc_info.value).lower()
    assert any(kw in error_msg for kw in ("truncate", "where", "not supported", "guard", "dangerous")), (
        f"Expected truncate-guard error message, got: {exc_info.value}"
    )
    # Verify all rows are still present
    count = kbc.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
    assert count == len(INITIAL_ROWS)


# ---------------------------------------------------------------------------
# 6. DELETE returns affected row count
# ---------------------------------------------------------------------------

def test_delete_returns_count(kbc, seeded_table):
    """DELETE should report the number of deleted rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    # We expect 2 rows to be deleted (id='2' and id='3', value='delete_me')
    result = kbc.execute(f"DELETE FROM {ref} WHERE value = 'delete_me';")
    affected = result.fetchone()
    if affected is not None:
        assert affected[0] == 2, (
            f"Expected 2 deleted rows, got {affected[0]}"
        )
    # Regardless of whether count is reported, verify the actual state
    count = kbc.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
    assert count == 3, f"Expected 3 rows remaining, got {count}"


# ---------------------------------------------------------------------------
# 7. DELETE on READ_ONLY attachment fails
# ---------------------------------------------------------------------------

def test_delete_readonly_fails(duckdb_con, keboola_token, keboola_url, seeded_table):
    """DELETE on a READ_ONLY attached database must raise an error."""
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_ro_del (
            TYPE keboola,
            TOKEN '{keboola_token}',
            READ_ONLY
        )
    """)
    try:
        ref = kbc_table_ref(seeded_table["table_id"]).replace("kbc.", "kbc_ro_del.", 1)
        with pytest.raises(Exception, match=r"(?i)read.?only"):
            duckdb_con.execute(f"DELETE FROM {ref} WHERE id = '1';")
    finally:
        try:
            duckdb_con.execute("DETACH kbc_ro_del;")
        except Exception:
            pass
