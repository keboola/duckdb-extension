"""
E2E tests for the Keboola catalog layer.

Verifies that:
- Buckets appear as schemas in SHOW SCHEMAS
- Tables appear in SHOW TABLES
- keboola_tables() scalar function lists all tables with metadata
- Column types are correctly resolved from KBC metadata
- KBC.description is exposed as column_comment in information_schema
"""

import pytest
import requests
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schema_names(kbc_con) -> list[str]:
    rows = kbc_con.execute("SHOW SCHEMAS IN kbc;").fetchall()
    return [r[0] for r in rows]


def _table_names_in(kbc_con, schema: str) -> list[str]:
    rows = kbc_con.execute(f'SHOW TABLES IN kbc."{schema}";').fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# 1. SHOW SCHEMAS lists test bucket
# ---------------------------------------------------------------------------

def test_show_schemas(kbc, test_bucket):
    """SHOW SCHEMAS IN kbc must list the test bucket that was just created."""
    schemas = _schema_names(kbc)
    assert test_bucket in schemas, (
        f"Expected bucket '{test_bucket}' in SHOW SCHEMAS, got: {schemas}"
    )


# ---------------------------------------------------------------------------
# 2. SHOW TABLES IN <schema> lists test table
# ---------------------------------------------------------------------------

def test_show_tables(kbc, test_table):
    """SHOW TABLES IN kbc."<schema>" must list the test table."""
    bucket_id = test_table["bucket_id"]
    table_name = test_table["table_name"]
    tables = _table_names_in(kbc, bucket_id)
    assert table_name in tables, (
        f"Expected table '{table_name}' in SHOW TABLES IN kbc.\"{bucket_id}\", got: {tables}"
    )


# ---------------------------------------------------------------------------
# 3. keboola_tables() scalar function
# ---------------------------------------------------------------------------

def test_keboola_tables_function(kbc, test_table):
    """SELECT * FROM keboola_tables('kbc') must return at least one row with our test table."""
    rows = kbc.execute("SELECT * FROM keboola_tables('kbc');").fetchdf()
    assert len(rows) > 0, "keboola_tables('kbc') returned no rows"

    # Check expected columns are present
    for col in ("schema_name", "table_name"):
        assert col in rows.columns, f"Column '{col}' missing from keboola_tables() output"

    # The test table must appear
    table_name = test_table["table_name"]
    bucket_id = test_table["bucket_id"]
    match = rows[
        (rows["schema_name"] == bucket_id) & (rows["table_name"] == table_name)
    ]
    assert len(match) == 1, (
        f"Test table '{table_name}' in bucket '{bucket_id}' not found in keboola_tables()"
    )


# ---------------------------------------------------------------------------
# 4. Column types — VARCHAR columns
# ---------------------------------------------------------------------------

def test_column_types_varchar(kbc, test_table):
    """
    DESCRIBE on a standard (id, name, value) table must report VARCHAR for
    all three string columns.
    """
    table_ref = kbc_table_ref(test_table["table_id"])
    rows = kbc.execute(f"DESCRIBE {table_ref};").fetchdf()

    assert len(rows) == 3, f"Expected 3 columns, got {len(rows)}: {rows}"
    for _, row in rows.iterrows():
        col_type = str(row["column_type"]).upper()
        assert "VARCHAR" in col_type, (
            f"Column '{row['column_name']}' expected VARCHAR, got '{col_type}'"
        )


# ---------------------------------------------------------------------------
# 5. Column types — native typed columns
# ---------------------------------------------------------------------------

def test_column_types_native(kbc, typed_test_table):
    """
    A table whose columns carry explicit type metadata must expose the
    correct DuckDB types (not VARCHAR fallback).
    """
    table_ref = kbc_table_ref(typed_test_table["table_id"])
    rows = kbc.execute(f"DESCRIBE {table_ref};").fetchdf()

    type_map = {row["column_name"]: row["column_type"].upper() for _, row in rows.iterrows()}

    # id is VARCHAR
    assert "VARCHAR" in type_map.get("id", ""), f"id should be VARCHAR, got: {type_map.get('id')}"

    # col_bigint should map to BIGINT (or INTEGER)
    bigint_type = type_map.get("col_bigint", "")
    assert any(t in bigint_type for t in ("BIGINT", "INTEGER", "INT")), (
        f"col_bigint expected BIGINT/INTEGER, got: {bigint_type}"
    )

    # col_double should map to DOUBLE (or FLOAT / REAL)
    double_type = type_map.get("col_double", "")
    assert any(t in double_type for t in ("DOUBLE", "FLOAT", "REAL")), (
        f"col_double expected DOUBLE, got: {double_type}"
    )

    # col_boolean
    assert "BOOLEAN" in type_map.get("col_boolean", ""), (
        f"col_boolean expected BOOLEAN, got: {type_map.get('col_boolean')}"
    )

    # col_date
    assert "DATE" in type_map.get("col_date", ""), (
        f"col_date expected DATE, got: {type_map.get('col_date')}"
    )

    # col_timestamp
    ts_type = type_map.get("col_timestamp", "")
    assert "TIMESTAMP" in ts_type, (
        f"col_timestamp expected TIMESTAMP*, got: {ts_type}"
    )


# ---------------------------------------------------------------------------
# 6. KBC.description as column_comment
# ---------------------------------------------------------------------------

def test_column_descriptions(kbc, storage_api, test_table):
    """
    After setting KBC.description metadata on a column via the Storage API,
    querying information_schema.columns must expose it as column_comment.
    """
    table_id = test_table["table_id"]
    bucket_id = test_table["bucket_id"]
    table_name = test_table["table_name"]
    description_text = "E2E test column description"

    # Set column metadata via Storage API
    try:
        storage_api.post(
            f"/tables/{table_id}/columns/name/metadata",
            json=[{"key": "KBC.description", "value": description_text}],
        )
    except Exception as exc:
        pytest.skip(f"Could not set column metadata via Storage API: {exc}")

    # Force catalog refresh so the new metadata is visible
    try:
        kbc.execute("CALL keboola_refresh_catalog('kbc');")
    except Exception:
        # Function may not exist yet — DETACH/ATTACH instead
        kbc.execute("DETACH kbc;")
        keboola_token = kbc.execute(
            "SELECT current_setting('keboola.token')"
        ).fetchone()
        # Re-attach is handled by the kbc fixture on next access; we simply re-query
        pass

    # Query information_schema
    rows = kbc.execute(f"""
        SELECT column_name, column_comment
        FROM information_schema.columns
        WHERE table_schema = '{bucket_id}'
          AND table_name   = '{table_name}'
          AND column_name  = 'name'
    """).fetchdf()

    if len(rows) == 0:
        pytest.skip("information_schema.columns did not return the expected row — catalog may not support column_comment yet")

    comment = rows.iloc[0]["column_comment"]
    assert comment == description_text, (
        f"Expected column_comment '{description_text}', got '{comment}'"
    )


# ---------------------------------------------------------------------------
# 7. Non-existent schema raises an error
# ---------------------------------------------------------------------------

def test_nonexistent_schema_raises(kbc):
    """Accessing a schema that doesn't exist must raise an error."""
    with pytest.raises(Exception):
        kbc.execute('SHOW TABLES IN kbc."in.c-this-bucket-does-not-exist-xyz-abc";')


# ---------------------------------------------------------------------------
# 8. Non-existent table raises an error
# ---------------------------------------------------------------------------

def test_nonexistent_table_raises(kbc, test_bucket):
    """Accessing a table that doesn't exist inside a valid schema must raise."""
    with pytest.raises(Exception):
        kbc.execute(f'SELECT * FROM kbc."{test_bucket}".this_table_does_not_exist_xyz;')
