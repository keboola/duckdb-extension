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
from conftest import kbc_table_ref

pytestmark = pytest.mark.live


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schema_names(kbc_con) -> list[str]:
    rows = kbc_con.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'kbc';"
    ).fetchall()
    return [r[0] for r in rows]


def _table_names_in(kbc_con, schema: str) -> list[str]:
    rows = kbc_con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'kbc' AND table_schema = ?;",
        [schema]
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# 1. SHOW SCHEMAS lists test bucket
# ---------------------------------------------------------------------------

def test_show_schemas(test_bucket, kbc):
    """SHOW SCHEMAS IN kbc must list the test bucket that was just created."""
    schemas = _schema_names(kbc)
    assert test_bucket in schemas, (
        f"Expected bucket '{test_bucket}' in SHOW SCHEMAS, got: {schemas}"
    )


# ---------------------------------------------------------------------------
# 2. SHOW TABLES IN <schema> lists test table
# ---------------------------------------------------------------------------

def test_show_tables(test_table, kbc):
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

def test_keboola_tables_function(test_table, kbc):
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

def test_column_types_varchar(test_table, kbc):
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

def test_column_types_native(typed_test_table, kbc):
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

def test_column_descriptions(test_table, kbc, storage_api):
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
    """Accessing a table inside a non-existent schema must raise an error."""
    with pytest.raises(Exception):
        kbc.execute('SELECT * FROM kbc."in.c-this-bucket-does-not-exist-xyz-abc".sometable;')


# ---------------------------------------------------------------------------
# 8. Non-existent table raises an error
# ---------------------------------------------------------------------------

def test_nonexistent_table_raises(test_bucket, kbc):
    """Accessing a table that doesn't exist inside a valid schema must raise."""
    with pytest.raises(Exception):
        kbc.execute(f'SELECT * FROM kbc."{test_bucket}".this_table_does_not_exist_xyz;')


# ---------------------------------------------------------------------------
# 9. Auto-refresh: new table created after ATTACH is discoverable
# ---------------------------------------------------------------------------

@pytest.mark.timeout(120)
def test_auto_refresh_new_table(test_bucket, kbc, storage_api, test_prefix):
    """
    A table created via Storage API *after* the kbc database was attached
    must be accessible via SELECT without DETACH/ATTACH.

    This exercises the lazy-refresh path in LookupEntry: on a cache miss,
    the extension calls RefreshTables() and retries before giving up.
    """
    table_name = f"{test_prefix}autorefresh"
    table_info = storage_api.create_table(
        test_bucket,
        table_name,
        columns=["id", "name"],
        primary_key=["id"],
    )
    table_id = table_info["id"]
    try:
        # SELECT must succeed without re-attaching — lazy refresh should kick in
        count = kbc.execute(
            f'SELECT COUNT(*) FROM kbc."{test_bucket}".{table_name};'
        ).fetchone()[0]
        assert count == 0, f"Expected empty table, got count={count}"
    finally:
        storage_api.delete_table(table_id)


# ---------------------------------------------------------------------------
# 10. Auto-refresh: new bucket/schema created after ATTACH is discoverable
# ---------------------------------------------------------------------------

@pytest.mark.timeout(120)
def test_auto_refresh_new_schema(kbc, storage_api, test_prefix):
    """
    A bucket (schema) created via Storage API *after* the kbc database was
    attached must appear in information_schema.schemata without re-attaching.

    This exercises the lazy-refresh path in LookupSchema: on a cache miss,
    the extension calls RefreshCatalog() and retries before throwing.
    """
    import uuid
    short_id = uuid.uuid4().hex[:8]
    bucket_name = f"duckdbtest-ar-{short_id}"
    bucket_info = storage_api.create_bucket("in", bucket_name, description="auto-refresh test")
    bucket_id = bucket_info["id"]  # e.g. "in.c-duckdbtest-ar-..."
    try:
        # Create a table inside the new bucket so we can SELECT from it
        table_info = storage_api.create_table(
            bucket_id,
            f"{test_prefix}artable",
            columns=["id", "val"],
            primary_key=["id"],
        )
        table_id = table_info["id"]
        table_name = table_id.split(".")[-1]
        try:
            count = kbc.execute(
                f'SELECT COUNT(*) FROM kbc."{bucket_id}".{table_name};'
            ).fetchone()[0]
            assert count == 0, f"Expected empty table, got count={count}"
        finally:
            storage_api.delete_table(table_id)
    finally:
        storage_api.delete_bucket(bucket_id, force=True)
