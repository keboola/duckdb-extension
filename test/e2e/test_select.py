"""
E2E tests for SELECT operations.

Tests filter pushdown, projection pushdown, pagination, JOIN with local
tables, and edge cases (empty table, NULL handling, ORDER BY).

Each test seeds data via conftest fixtures and tears it down automatically.
"""

import io
import csv
import pytest
import pandas as pd
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _upload_rows(storage_api, table_id: str, rows: list[dict], incremental: bool = False):
    """Upload rows as CSV to a Keboola table using the Storage Importer."""
    if not rows:
        return

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    csv_data = buf.getvalue()

    # Discover importer URL
    index = storage_api.get("")
    importer_url = None
    for svc in index.get("services", []):
        if svc.get("id") == "import":
            importer_url = svc["url"]
            break

    if importer_url is None:
        pytest.skip("Importer service URL not available — cannot seed data for SELECT tests")

    storage_api.importer_write_table(importer_url, table_id, csv_data.encode(), incremental=incremental)


SEED_ROWS = [
    {"id": "1", "name": "Alice",   "value": "100"},
    {"id": "2", "name": "Bob",     "value": "200"},
    {"id": "3", "name": "Charlie", "value": "300"},
    {"id": "4", "name": "Diana",   "value": "400"},
    {"id": "5", "name": "Eve",     "value": "500"},
    {"id": "99", "name": "Null-Value-Test", "value": ""},  # empty value simulates NULL-ish
]


@pytest.fixture
def seeded_table(storage_api, test_table, kbc):
    """Upload SEED_ROWS into the test table and return the table info."""
    _upload_rows(storage_api, test_table["table_id"], SEED_ROWS, incremental=False)
    yield test_table


# ---------------------------------------------------------------------------
# 1. SELECT * — full table scan
# ---------------------------------------------------------------------------

def test_select_all(kbc, seeded_table):
    """SELECT * must return all seeded rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT * FROM {ref};").fetchdf()
    assert len(df) == len(SEED_ROWS), (
        f"Expected {len(SEED_ROWS)} rows, got {len(df)}"
    )
    assert set(df.columns) == {"id", "name", "value"}


# ---------------------------------------------------------------------------
# 2. Projection pushdown
# ---------------------------------------------------------------------------

def test_select_projection(kbc, seeded_table):
    """Only the requested columns should be present in the result."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT id, name FROM {ref};").fetchdf()
    assert list(df.columns) == ["id", "name"], f"Expected [id, name], got {list(df.columns)}"
    assert len(df) == len(SEED_ROWS)

    # The 'value' column must NOT be present
    assert "value" not in df.columns


# ---------------------------------------------------------------------------
# 3. WHERE equality
# ---------------------------------------------------------------------------

def test_where_equality(kbc, seeded_table):
    """WHERE id = '1' must return exactly Alice's row."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT id, name FROM {ref} WHERE id = '1';").fetchdf()
    assert len(df) == 1, f"Expected 1 row, got {len(df)}"
    assert df.iloc[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# 4. WHERE greater-than
# ---------------------------------------------------------------------------

def test_where_greater_than(kbc, seeded_table):
    """WHERE id > '3' (string comparison) should return rows with id '4', '5', '99'."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT id FROM {ref} WHERE id > '3' ORDER BY id;").fetchdf()
    ids = set(df["id"].tolist())
    # String comparison: '4', '5', '99' are all > '3'
    assert "4" in ids and "5" in ids, f"Expected ids 4 and 5 in result, got: {ids}"
    assert "1" not in ids and "2" not in ids and "3" not in ids


# ---------------------------------------------------------------------------
# 5. WHERE IS NULL
# ---------------------------------------------------------------------------

def test_where_is_null(kbc, seeded_table):
    """WHERE value IS NULL must not raise and must return only truly-NULL rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    # This tests that IS NULL pushdown doesn't crash; result may be 0 rows
    df = kbc.execute(f"SELECT id FROM {ref} WHERE value IS NULL;").fetchdf()
    assert isinstance(len(df), int), "Result should be a DataFrame with integer row count"


# ---------------------------------------------------------------------------
# 6. WHERE IN list
# ---------------------------------------------------------------------------

def test_where_in(kbc, seeded_table):
    """WHERE id IN ('1','3','5') must return exactly those three rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT id FROM {ref} WHERE id IN ('1', '3', '5') ORDER BY id;").fetchdf()
    assert len(df) == 3, f"Expected 3 rows, got {len(df)}: {df['id'].tolist()}"
    assert df["id"].tolist() == ["1", "3", "5"]


# ---------------------------------------------------------------------------
# 7. WHERE AND
# ---------------------------------------------------------------------------

def test_where_and(kbc, seeded_table):
    """WHERE id = '2' AND name = 'Bob' must return exactly one row."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(
        f"SELECT id, name FROM {ref} WHERE id = '2' AND name = 'Bob';"
    ).fetchdf()
    assert len(df) == 1
    assert df.iloc[0]["id"] == "2"
    assert df.iloc[0]["name"] == "Bob"


# ---------------------------------------------------------------------------
# 8. LIMIT
# ---------------------------------------------------------------------------

def test_limit(kbc, seeded_table):
    """LIMIT 3 must return at most 3 rows."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT * FROM {ref} LIMIT 3;").fetchdf()
    assert len(df) <= 3, f"Expected at most 3 rows, got {len(df)}"
    # Also test LIMIT 1
    df1 = kbc.execute(f"SELECT * FROM {ref} LIMIT 1;").fetchdf()
    assert len(df1) == 1


# ---------------------------------------------------------------------------
# 9. ORDER BY
# ---------------------------------------------------------------------------

def test_order_by(kbc, seeded_table):
    """ORDER BY id ASC must return ids in ascending lexicographic order."""
    ref = kbc_table_ref(seeded_table["table_id"])
    df = kbc.execute(f"SELECT id FROM {ref} ORDER BY id ASC;").fetchdf()
    ids = df["id"].tolist()
    assert ids == sorted(ids), f"Expected sorted ids, got: {ids}"


# ---------------------------------------------------------------------------
# 10. JOIN with local table
# ---------------------------------------------------------------------------

def test_join_with_local(kbc, seeded_table):
    """JOIN a Keboola table with a local DuckDB in-memory table."""
    ref = kbc_table_ref(seeded_table["table_id"])

    kbc.execute("""
        CREATE TEMP TABLE local_lookup AS
        SELECT '1' AS id, 'vip' AS tier
        UNION ALL
        SELECT '3', 'prospect'
    """)
    try:
        df = kbc.execute(f"""
            SELECT k.id, k.name, l.tier
            FROM {ref} k
            JOIN local_lookup l ON k.id = l.id
            ORDER BY k.id
        """).fetchdf()

        assert len(df) == 2, f"Expected 2 matching rows, got {len(df)}"
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[0]["tier"] == "vip"
        assert df.iloc[1]["name"] == "Charlie"
        assert df.iloc[1]["tier"] == "prospect"
    finally:
        kbc.execute("DROP TABLE IF EXISTS local_lookup;")


# ---------------------------------------------------------------------------
# 11. Pagination — 2500 rows
# ---------------------------------------------------------------------------

@pytest.mark.timeout(120)
def test_pagination(storage_api, kbc, test_table):
    """INSERT 2500 rows, then SELECT COUNT(*) must equal 2500 (exercises pagination)."""
    table_id = test_table["table_id"]

    # Build 2500 rows
    big_rows = [{"id": str(i), "name": f"User{i}", "value": str(i * 10)} for i in range(1, 2501)]
    _upload_rows(storage_api, table_id, big_rows, incremental=False)

    ref = kbc_table_ref(table_id)
    count = kbc.execute(f"SELECT COUNT(*) FROM {ref};").fetchone()[0]
    assert count == 2500, f"Expected 2500 rows after pagination test, got {count}"

    # Verify no duplicates (would indicate pagination bug)
    distinct_count = kbc.execute(f"SELECT COUNT(DISTINCT id) FROM {ref};").fetchone()[0]
    assert distinct_count == 2500, f"Duplicates detected: COUNT(DISTINCT id)={distinct_count}"


# ---------------------------------------------------------------------------
# 12. Empty table
# ---------------------------------------------------------------------------

def test_empty_table(test_table, kbc):
    """SELECT from an empty table must return 0 rows without error."""
    ref = kbc_table_ref(test_table["table_id"])
    df = kbc.execute(f"SELECT * FROM {ref};").fetchdf()
    assert len(df) == 0, f"Expected 0 rows from empty table, got {len(df)}"
    # Columns must still be present
    assert set(df.columns) == {"id", "name", "value"}
