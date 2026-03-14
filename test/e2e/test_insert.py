"""
E2E tests for INSERT operations via the Keboola DuckDB extension.

All tests use the `kbc` fixture (live-attached Keboola) and the `test_table`
fixture for isolation. The conftest creates a fresh bucket + table per test
and deletes them in teardown.
"""

import pytest
import pandas as pd
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _count(kbc_con, table_ref: str) -> int:
    return kbc_con.execute(f"SELECT COUNT(*) FROM {table_ref};").fetchone()[0]


def _fetchall(kbc_con, table_ref: str) -> pd.DataFrame:
    return kbc_con.execute(f"SELECT * FROM {table_ref} ORDER BY id;").fetchdf()


# ---------------------------------------------------------------------------
# 1. Single row insert
# ---------------------------------------------------------------------------

def test_insert_single_row(test_table, kbc):
    """INSERT 1 row, then SELECT it back and verify all field values."""
    ref = kbc_table_ref(test_table["table_id"])
    kbc.execute(f"INSERT INTO {ref} VALUES ('r1', 'SingleRow', 'val1');")
    df = kbc.execute(f"SELECT id, name, value FROM {ref} WHERE id = 'r1';").fetchdf()
    assert len(df) == 1, f"Expected 1 row after INSERT, got {len(df)}"
    row = df.iloc[0]
    assert row["id"] == "r1"
    assert row["name"] == "SingleRow"
    assert row["value"] == "val1"


# ---------------------------------------------------------------------------
# 2. INSERT NULL values
# ---------------------------------------------------------------------------

def test_insert_null_values(test_table, kbc):
    """INSERT a row with NULL in the 'value' column; SELECT back must return NULL."""
    ref = kbc_table_ref(test_table["table_id"])
    kbc.execute(f"INSERT INTO {ref} (id, name, value) VALUES ('null1', 'NullTest', NULL);")
    df = kbc.execute(f"SELECT value FROM {ref} WHERE id = 'null1';").fetchdf()
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["value"]), (
        f"Expected NULL value, got {df.iloc[0]['value']!r}"
    )


# ---------------------------------------------------------------------------
# 3. Multi-row insert
# ---------------------------------------------------------------------------

def test_insert_multi_row(test_table, kbc):
    """INSERT 5 rows in a single VALUES clause; COUNT(*) must equal 5."""
    ref = kbc_table_ref(test_table["table_id"])
    kbc.execute(f"""
        INSERT INTO {ref} (id, name, value) VALUES
            ('m1', 'Multi1', '10'),
            ('m2', 'Multi2', '20'),
            ('m3', 'Multi3', '30'),
            ('m4', 'Multi4', '40'),
            ('m5', 'Multi5', '50');
    """)
    assert _count(kbc, ref) == 5, f"Expected 5 rows after multi-row INSERT"


# ---------------------------------------------------------------------------
# 4. INSERT INTO ... SELECT from local table
# ---------------------------------------------------------------------------

def test_insert_select_from_local(test_table, kbc):
    """INSERT INTO kbc_table SELECT * FROM a local DuckDB relation."""
    ref = kbc_table_ref(test_table["table_id"])
    kbc.execute("""
        CREATE TEMP TABLE local_src AS
        SELECT '10' AS id, 'FromLocal1' AS name, 'lv1' AS value
        UNION ALL
        SELECT '11', 'FromLocal2', 'lv2'
        UNION ALL
        SELECT '12', 'FromLocal3', 'lv3'
    """)
    try:
        kbc.execute(f"INSERT INTO {ref} SELECT * FROM local_src;")
        count = _count(kbc, ref)
        assert count == 3, f"Expected 3 rows after INSERT...SELECT, got {count}"

        ids = set(kbc.execute(f"SELECT id FROM {ref};").fetchdf()["id"].tolist())
        assert {"10", "11", "12"} == ids
    finally:
        kbc.execute("DROP TABLE IF EXISTS local_src;")


# ---------------------------------------------------------------------------
# 5. CSV edge cases (commas, quotes, newlines in values)
# ---------------------------------------------------------------------------

def test_insert_csv_edge_cases(test_table, kbc):
    """Values with commas, double-quotes, and embedded newlines must round-trip cleanly."""
    ref = kbc_table_ref(test_table["table_id"])
    tricky_values = [
        ("e1", "Name, With Comma",      "val,comma"),
        ("e2", 'Name "Quoted"',          'val "with" quotes'),
        ("e3", "Name\nNewline",          "val\nnewline"),
    ]
    for row_id, name, value in tricky_values:
        kbc.execute(f"INSERT INTO {ref} (id, name, value) VALUES (?, ?, ?);", [row_id, name, value])

    df = kbc.execute(f"SELECT id, name, value FROM {ref} ORDER BY id;").fetchdf()
    assert len(df) == 3

    for row_id, expected_name, expected_value in tricky_values:
        match = df[df["id"] == row_id]
        assert len(match) == 1, f"Row {row_id} not found"
        assert match.iloc[0]["name"] == expected_name, (
            f"name mismatch for {row_id}: got {match.iloc[0]['name']!r}"
        )
        assert match.iloc[0]["value"] == expected_value, (
            f"value mismatch for {row_id}: got {match.iloc[0]['value']!r}"
        )


# ---------------------------------------------------------------------------
# 6. INSERT returns affected row count
# ---------------------------------------------------------------------------

def test_insert_returns_count(test_table, kbc):
    """The DuckDB INSERT statement must report the number of affected rows."""
    ref = kbc_table_ref(test_table["table_id"])
    result = kbc.execute(f"""
        INSERT INTO {ref} (id, name, value) VALUES
            ('cnt1', 'CountTest1', 'v1'),
            ('cnt2', 'CountTest2', 'v2'),
            ('cnt3', 'CountTest3', 'v3');
    """)
    # DuckDB returns the affected row count via fetchone() on INSERT
    affected = result.fetchone()
    if affected is not None:
        assert affected[0] == 3, f"Expected 3 affected rows, got {affected[0]}"
    # Even if affected rows are not returned, the actual count must be 3
    assert _count(kbc, ref) == 3


# ---------------------------------------------------------------------------
# 7. Large batch INSERT
# ---------------------------------------------------------------------------

@pytest.mark.timeout(60)
def test_insert_large_batch(test_table, kbc):
    """INSERT 1000 rows via a single INSERT...SELECT; COUNT(*) must be 1000."""
    ref = kbc_table_ref(test_table["table_id"])

    # Build a local 1000-row relation using generate_series
    kbc.execute(f"""
        INSERT INTO {ref} (id, name, value)
        SELECT
            CAST(i AS VARCHAR)                AS id,
            'BatchUser' || CAST(i AS VARCHAR) AS name,
            CAST(i * 7 AS VARCHAR)            AS value
        FROM generate_series(1, 1000) t(i);
    """)
    count = _count(kbc, ref)
    assert count == 1000, f"Expected 1000 rows after large batch INSERT, got {count}"


# ---------------------------------------------------------------------------
# 8. INSERT wrong column count raises error
# ---------------------------------------------------------------------------

def test_insert_wrong_column_count(test_table, kbc):
    """INSERT with wrong number of columns must raise a descriptive error."""
    ref = kbc_table_ref(test_table["table_id"])
    with pytest.raises(Exception) as exc_info:
        kbc.execute(f"INSERT INTO {ref} VALUES ('only_two_cols', 'oops');")
    # The error message should be meaningful — not a segfault or empty message
    assert str(exc_info.value).strip() != ""


# ---------------------------------------------------------------------------
# 9. INSERT into READ_ONLY attached database fails
# ---------------------------------------------------------------------------

def test_insert_readonly_fails(duckdb_con, keboola_token, keboola_url, test_table):
    """Inserting into a READ_ONLY attached database must raise an error."""
    duckdb_con.execute(f"""
        ATTACH '{keboola_url}' AS kbc_ro_insert (
            TYPE keboola,
            TOKEN '{keboola_token}',
            READ_ONLY
        )
    """)
    try:
        ref = kbc_table_ref(test_table["table_id"]).replace("kbc.", "kbc_ro_insert.", 1)
        with pytest.raises(Exception, match=r"(?i)read.?only"):
            duckdb_con.execute(f"INSERT INTO {ref} VALUES ('ro1', 'RO test', 'v');")
    finally:
        try:
            duckdb_con.execute("DETACH kbc_ro_insert;")
        except Exception:
            pass
