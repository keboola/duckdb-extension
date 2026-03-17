"""
06 — Data Quality Checks & CI-Friendly Assertions
===================================================

Demonstrates:
- Attaching in read-only mode for safe validation
- Row count checks
- NULL / NOT NULL validation
- Uniqueness checks
- Value-range and referential integrity assertions
- pytest-compatible test functions for CI pipelines

Run standalone:
    python examples/06_data_quality.py

Run with pytest (all assertions are test functions):
    pytest examples/06_data_quality.py -v

Prerequisites:
    pip install duckdb pytest

Before running, replace placeholder values and schema/table names.
"""

import os
import sys
import duckdb
import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")

# Replace with real bucket/table names from your project
SCHEMA = "in.c-crm"
TABLE = "contacts"


# ---------------------------------------------------------------------------
# Shared connection fixture (pytest) / helper (standalone)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def kbc_con():
    """Create a read-only DuckDB connection to Keboola for validation."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")
    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE      keboola,
            TOKEN     '{KEBOOLA_TOKEN}',
            READ_ONLY true
        );
    """)
    yield con
    con.execute("DETACH kbc;")
    con.close()


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Standalone helper — returns a read-only Keboola connection."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")
    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE      keboola,
            TOKEN     '{KEBOOLA_TOKEN}',
            READ_ONLY true
        );
    """)
    return con


TBL = f'kbc."{SCHEMA}".{TABLE}'


# ---------------------------------------------------------------------------
# 1. Row count check — table is not empty
# ---------------------------------------------------------------------------

def test_table_not_empty(kbc_con):
    """The table must contain at least one row."""
    count = kbc_con.execute(f"SELECT COUNT(*) FROM {TBL};").fetchone()[0]
    assert count > 0, f"Table {TBL} is empty (0 rows)"


# ---------------------------------------------------------------------------
# 2. Row count within expected range
# ---------------------------------------------------------------------------

EXPECTED_MIN_ROWS = 10
EXPECTED_MAX_ROWS = 1_000_000

def test_row_count_in_range(kbc_con):
    """Row count must fall within the expected range."""
    count = kbc_con.execute(f"SELECT COUNT(*) FROM {TBL};").fetchone()[0]
    assert EXPECTED_MIN_ROWS <= count <= EXPECTED_MAX_ROWS, (
        f"Row count {count} outside expected range "
        f"[{EXPECTED_MIN_ROWS}, {EXPECTED_MAX_ROWS}]"
    )


# ---------------------------------------------------------------------------
# 3. No NULL values in required columns
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ["id", "name"]

def test_no_nulls_in_required_columns(kbc_con):
    """Required columns must not contain NULL values."""
    for col in REQUIRED_COLUMNS:
        null_count = kbc_con.execute(f"""
            SELECT COUNT(*) FROM {TBL} WHERE "{col}" IS NULL;
        """).fetchone()[0]
        assert null_count == 0, (
            f"Column '{col}' has {null_count} NULL values in {TBL}"
        )


# ---------------------------------------------------------------------------
# 4. Uniqueness check on primary key
# ---------------------------------------------------------------------------

PK_COLUMN = "id"

def test_primary_key_unique(kbc_con):
    """Primary key column must have no duplicate values."""
    result = kbc_con.execute(f"""
        SELECT "{PK_COLUMN}", COUNT(*) AS cnt
        FROM {TBL}
        GROUP BY "{PK_COLUMN}"
        HAVING cnt > 1
        LIMIT 5;
    """).fetchall()
    assert len(result) == 0, (
        f"Duplicate values found in '{PK_COLUMN}': "
        f"{[r[0] for r in result]}"
    )


# ---------------------------------------------------------------------------
# 5. No completely empty rows (all columns NULL)
# ---------------------------------------------------------------------------

def test_no_fully_empty_rows(kbc_con):
    """No row should have all columns set to NULL."""
    # Build a WHERE clause that checks all columns are NULL
    cols = kbc_con.execute(f"DESCRIBE {TBL};").fetchall()
    col_names = [c[0] for c in cols]
    all_null = " AND ".join(f'"{c}" IS NULL' for c in col_names)

    empty_count = kbc_con.execute(f"""
        SELECT COUNT(*) FROM {TBL} WHERE {all_null};
    """).fetchone()[0]
    assert empty_count == 0, (
        f"Found {empty_count} completely empty rows in {TBL}"
    )


# ---------------------------------------------------------------------------
# 6. Value freshness — most recent record not older than N days
# ---------------------------------------------------------------------------

# Set to a timestamp column and max staleness in days
TIMESTAMP_COLUMN = "updated_at"
MAX_STALENESS_DAYS = 30

def test_data_freshness(kbc_con):
    """The most recent record must not be older than MAX_STALENESS_DAYS."""
    try:
        result = kbc_con.execute(f"""
            SELECT MAX("{TIMESTAMP_COLUMN}") FROM {TBL};
        """).fetchone()[0]
    except Exception:
        pytest.skip(f"Column '{TIMESTAMP_COLUMN}' not found — skipping freshness check")
        return

    if result is None:
        pytest.skip("No data to check freshness against")
        return

    age_days = kbc_con.execute(f"""
        SELECT DATE_DIFF('day', MAX("{TIMESTAMP_COLUMN}")::TIMESTAMP, CURRENT_TIMESTAMP)
        FROM {TBL};
    """).fetchone()[0]
    assert age_days <= MAX_STALENESS_DAYS, (
        f"Data is {age_days} days old (max allowed: {MAX_STALENESS_DAYS})"
    )


# ---------------------------------------------------------------------------
# Standalone runner — executes all checks and reports results
# ---------------------------------------------------------------------------

def main():
    """Run all data quality checks and print a summary report."""
    con = _get_connection()
    checks = [
        ("Table not empty",             test_table_not_empty),
        ("Row count in range",          test_row_count_in_range),
        ("No NULLs in required cols",   test_no_nulls_in_required_columns),
        ("Primary key unique",          test_primary_key_unique),
        ("No fully empty rows",         test_no_fully_empty_rows),
        ("Data freshness",              test_data_freshness),
    ]

    passed = 0
    failed = 0
    skipped = 0

    print(f"Running {len(checks)} data quality checks on {TBL}\n")
    print("-" * 60)

    for name, check_fn in checks:
        try:
            check_fn(con)
            print(f"  PASS  {name}")
            passed += 1
        except pytest.skip.Exception:
            print(f"  SKIP  {name}")
            skipped += 1
        except AssertionError as e:
            print(f"  FAIL  {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {name}: {e}")
            failed += 1

    print("-" * 60)
    print(f"\nResults: {passed} passed, {failed} failed, {skipped} skipped")

    con.execute("DETACH kbc;")
    con.close()

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
