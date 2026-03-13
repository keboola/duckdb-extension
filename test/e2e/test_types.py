"""
E2E type round-trip tests.

For each supported DuckDB/Keboola type:
1. INSERT a representative value via SQL (parameterized query)
2. SELECT it back
3. Assert the Python value matches the expected type and value

Tests use the `typed_test_table` fixture which creates a table with
explicitly-typed columns (BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, etc.)
and a VARCHAR fallback table for string-typed columns.
"""

import datetime
import decimal
import math
import pytest
import pandas as pd
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_and_fetch(kbc_con, ref: str, row_id: str, col: str, value) -> object:
    """Insert a single value into `col` (all others NULL) and fetch it back."""
    kbc_con.execute(
        f"INSERT INTO {ref} (id, {col}) VALUES (?, ?);",
        [row_id, value],
    )
    df = kbc_con.execute(f"SELECT {col} FROM {ref} WHERE id = ?;", [row_id]).fetchdf()
    assert len(df) == 1, f"Expected 1 row for id={row_id!r}"
    return df.iloc[0][col]


def _insert_null_and_fetch(kbc_con, ref: str, row_id: str, col: str) -> object:
    """Insert NULL into `col` and fetch it back."""
    kbc_con.execute(
        f"INSERT INTO {ref} (id, {col}) VALUES (?, NULL);",
        [row_id],
    )
    df = kbc_con.execute(f"SELECT {col} FROM {ref} WHERE id = ?;", [row_id]).fetchdf()
    assert len(df) == 1
    return df.iloc[0][col]


# ---------------------------------------------------------------------------
# VARCHAR
# ---------------------------------------------------------------------------

def test_type_varchar(kbc, test_table):
    """VARCHAR round-trip: str → VARCHAR → str."""
    ref = kbc_table_ref(test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_varchar", "name", "hello world")
    assert isinstance(val, str), f"Expected str, got {type(val)}"
    assert val == "hello world"


def test_type_varchar_unicode(kbc, test_table):
    """VARCHAR handles unicode characters correctly."""
    ref = kbc_table_ref(test_table["table_id"])
    unicode_val = "こんにちは 🎉 Ñoño"
    val = _insert_and_fetch(kbc, ref, "type_varchar_unicode", "name", unicode_val)
    assert val == unicode_val, f"Unicode round-trip failed: got {val!r}"


# ---------------------------------------------------------------------------
# BIGINT
# ---------------------------------------------------------------------------

def test_type_bigint(kbc, typed_test_table):
    """BIGINT round-trip: int → BIGINT → int (or numpy int64)."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_bigint", "col_bigint", 9_007_199_254_740_993)
    assert int(val) == 9_007_199_254_740_993, f"BIGINT round-trip failed: got {val!r}"


def test_type_bigint_negative(kbc, typed_test_table):
    """BIGINT handles negative values."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_bigint_neg", "col_bigint", -42_000_000_000)
    assert int(val) == -42_000_000_000


# ---------------------------------------------------------------------------
# DECIMAL (via VARCHAR column + CAST)
# ---------------------------------------------------------------------------

def test_type_decimal(kbc, test_table):
    """
    DECIMAL round-trip via a typed column or explicit CAST.
    Using the standard test_table (VARCHAR) and CAST to verify numeric accuracy.
    """
    ref = kbc_table_ref(test_table["table_id"])
    kbc.execute(f"INSERT INTO {ref} (id, value) VALUES ('dec_test', '123456.789');")
    df = kbc.execute(
        f"SELECT CAST(value AS DECIMAL(12,3)) AS dec_val FROM {ref} WHERE id = 'dec_test';"
    ).fetchdf()
    assert len(df) == 1
    result = decimal.Decimal(str(df.iloc[0]["dec_val"]))
    expected = decimal.Decimal("123456.789")
    assert result == expected, f"DECIMAL round-trip failed: got {result!r}"


# ---------------------------------------------------------------------------
# DOUBLE
# ---------------------------------------------------------------------------

def test_type_double(kbc, typed_test_table):
    """DOUBLE round-trip: float → DOUBLE → float."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_double", "col_double", 3.141592653589793)
    assert isinstance(float(val), float)
    assert math.isclose(float(val), 3.141592653589793, rel_tol=1e-9), (
        f"DOUBLE round-trip failed: got {val!r}"
    )


def test_type_double_negative(kbc, typed_test_table):
    """DOUBLE handles negative float values."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_double_neg", "col_double", -0.000123456789)
    assert math.isclose(float(val), -0.000123456789, rel_tol=1e-7)


# ---------------------------------------------------------------------------
# BOOLEAN
# ---------------------------------------------------------------------------

def test_type_boolean_true(kbc, typed_test_table):
    """BOOLEAN True round-trip."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_bool_t", "col_boolean", True)
    assert bool(val) is True, f"Expected True, got {val!r}"


def test_type_boolean_false(kbc, typed_test_table):
    """BOOLEAN False round-trip."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_and_fetch(kbc, ref, "type_bool_f", "col_boolean", False)
    assert bool(val) is False, f"Expected False, got {val!r}"


# ---------------------------------------------------------------------------
# DATE
# ---------------------------------------------------------------------------

def test_type_date(kbc, typed_test_table):
    """DATE round-trip: date → DATE → datetime.date."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    expected = datetime.date(2024, 6, 15)
    val = _insert_and_fetch(kbc, ref, "type_date", "col_date", expected)
    # DuckDB/pandas may return datetime.date or Timestamp — normalize
    if hasattr(val, "date"):
        val = val.date()
    assert val == expected, f"DATE round-trip failed: got {val!r}"


def test_type_date_epoch(kbc, typed_test_table):
    """DATE handles Unix epoch boundary (1970-01-01)."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    expected = datetime.date(1970, 1, 1)
    val = _insert_and_fetch(kbc, ref, "type_date_epoch", "col_date", expected)
    if hasattr(val, "date"):
        val = val.date()
    assert val == expected


# ---------------------------------------------------------------------------
# TIMESTAMP
# ---------------------------------------------------------------------------

def test_type_timestamp(kbc, typed_test_table):
    """TIMESTAMP round-trip: datetime → TIMESTAMP → datetime.datetime."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    expected = datetime.datetime(2024, 3, 14, 9, 26, 53, 0)  # microseconds=0 for portability
    val = _insert_and_fetch(kbc, ref, "type_ts", "col_timestamp", expected)
    # Convert pandas Timestamp to datetime if needed
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime().replace(tzinfo=None)
    assert isinstance(val, datetime.datetime), f"Expected datetime, got {type(val)}"
    assert val == expected, f"TIMESTAMP round-trip failed: got {val!r}"


def test_type_timestamp_microsecond(kbc, typed_test_table):
    """TIMESTAMP preserves microsecond precision."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    expected = datetime.datetime(2025, 12, 31, 23, 59, 59, 123456)
    val = _insert_and_fetch(kbc, ref, "type_ts_us", "col_timestamp", expected)
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime().replace(tzinfo=None)
    assert val == expected, f"TIMESTAMP microsecond precision failed: got {val!r}"


# ---------------------------------------------------------------------------
# NULL round-trips for every type
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("col,row_id", [
    ("col_varchar",   "null_varchar"),
    ("col_bigint",    "null_bigint"),
    ("col_double",    "null_double"),
    ("col_boolean",   "null_boolean"),
    ("col_date",      "null_date"),
    ("col_timestamp", "null_timestamp"),
])
def test_type_null_each_type(kbc, typed_test_table, col, row_id):
    """NULL must be stored and retrieved as NULL (not 0, False, empty string, etc.)."""
    ref = kbc_table_ref(typed_test_table["table_id"])
    val = _insert_null_and_fetch(kbc, ref, row_id, col)
    assert pd.isna(val) or val is None, (
        f"Expected NULL for column '{col}', got {val!r} ({type(val).__name__})"
    )


def test_type_null_varchar(kbc, test_table):
    """NULL in a plain VARCHAR column round-trips as NULL."""
    ref = kbc_table_ref(test_table["table_id"])
    val = _insert_null_and_fetch(kbc, ref, "null_vc", "name")
    assert pd.isna(val) or val is None, f"Expected NULL for VARCHAR, got {val!r}"


# ---------------------------------------------------------------------------
# Empty string vs NULL distinction
# ---------------------------------------------------------------------------

def test_empty_string_is_not_null(kbc, test_table):
    """Empty string '' must be stored as '' and NOT converted to NULL."""
    ref = kbc_table_ref(test_table["table_id"])
    _insert_and_fetch(kbc, ref, "empty_str", "name", "")
    df = kbc.execute(f"SELECT name, name IS NULL AS is_null FROM {ref} WHERE id = 'empty_str';").fetchdf()
    assert len(df) == 1
    # Empty string should NOT be NULL
    assert not df.iloc[0]["is_null"], (
        "Empty string '' was stored as NULL — should be a distinct empty value"
    )
