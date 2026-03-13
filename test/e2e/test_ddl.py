"""
E2E tests for DDL operations (CREATE TABLE, DROP TABLE, CREATE SCHEMA / bucket).

Each test creates and tears down its own resources.  The conftest fixtures
handle bucket/table cleanup; additional DDL objects created inside tests
are cleaned up in try/finally blocks.
"""

import pytest
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
# 1. CREATE SCHEMA (creates a Keboola bucket)
# ---------------------------------------------------------------------------

def test_create_schema(kbc, test_prefix):
    """CREATE SCHEMA must create a new Keboola bucket and appear in SHOW SCHEMAS."""
    # bucket name: "in.c-ddltest-<prefix>"
    schema_name = f"in.c-ddltest-{test_prefix.replace('_', '-').rstrip('-')}"
    kbc.execute(f'CREATE SCHEMA kbc."{schema_name}";')
    try:
        schemas = _schema_names(kbc)
        assert schema_name in schemas, (
            f"Created schema '{schema_name}' not found in SHOW SCHEMAS: {schemas}"
        )
    finally:
        # Drop the bucket — use DROP SCHEMA or Storage API cleanup
        try:
            kbc.execute(f'DROP SCHEMA kbc."{schema_name}";')
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 2. CREATE TABLE inside existing schema
# ---------------------------------------------------------------------------

def test_create_table(kbc, test_bucket, test_prefix):
    """CREATE TABLE must create the table in Keboola and appear in SHOW TABLES."""
    table_name = f"{test_prefix}ddl_create"
    kbc.execute(f"""
        CREATE TABLE kbc."{test_bucket}".{table_name} (
            id    VARCHAR,
            label VARCHAR,
            score VARCHAR
        );
    """)
    try:
        tables = _table_names_in(kbc, test_bucket)
        assert table_name in tables, (
            f"Table '{table_name}' not found in SHOW TABLES for '{test_bucket}': {tables}"
        )
    finally:
        try:
            kbc.execute(f'DROP TABLE kbc."{test_bucket}".{table_name};')
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 3. CREATE TABLE then INSERT + SELECT (full round trip)
# ---------------------------------------------------------------------------

def test_create_table_then_insert_select(kbc, test_bucket, test_prefix):
    """Create a table, insert rows, and read them back."""
    table_name = f"{test_prefix}ddl_roundtrip"
    ref = f'kbc."{test_bucket}".{table_name}'
    kbc.execute(f"CREATE TABLE {ref} (id VARCHAR, msg VARCHAR);")
    try:
        kbc.execute(f"INSERT INTO {ref} VALUES ('x1', 'hello'), ('x2', 'world');")
        df = kbc.execute(f"SELECT * FROM {ref} ORDER BY id;").fetchdf()
        assert len(df) == 2
        assert df.iloc[0]["id"] == "x1" and df.iloc[0]["msg"] == "hello"
        assert df.iloc[1]["id"] == "x2" and df.iloc[1]["msg"] == "world"
    finally:
        try:
            kbc.execute(f"DROP TABLE {ref};")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 4. DROP TABLE removes it from SHOW TABLES
# ---------------------------------------------------------------------------

def test_drop_table(kbc, test_bucket, test_prefix):
    """DROP TABLE must remove the table from Keboola and from SHOW TABLES."""
    table_name = f"{test_prefix}ddl_drop"
    ref = f'kbc."{test_bucket}".{table_name}'
    kbc.execute(f"CREATE TABLE {ref} (id VARCHAR);")
    # Confirm it exists
    tables_before = _table_names_in(kbc, test_bucket)
    assert table_name in tables_before, "Table must exist before DROP"

    kbc.execute(f"DROP TABLE {ref};")

    tables_after = _table_names_in(kbc, test_bucket)
    assert table_name not in tables_after, (
        f"Table '{table_name}' still present after DROP TABLE"
    )


# ---------------------------------------------------------------------------
# 5. DROP SCHEMA removes the bucket
# ---------------------------------------------------------------------------

def test_drop_schema(kbc, test_prefix):
    """DROP SCHEMA (empty bucket) must remove it from SHOW SCHEMAS."""
    schema_name = f"in.c-droptest-{test_prefix.replace('_', '-').rstrip('-')}"
    kbc.execute(f'CREATE SCHEMA kbc."{schema_name}";')

    # Verify it was created
    schemas_before = _schema_names(kbc)
    assert schema_name in schemas_before, "Schema must exist before DROP SCHEMA"

    kbc.execute(f'DROP SCHEMA kbc."{schema_name}";')

    schemas_after = _schema_names(kbc)
    assert schema_name not in schemas_after, (
        f"Schema '{schema_name}' still present after DROP SCHEMA"
    )


# ---------------------------------------------------------------------------
# 6. CREATE TABLE with typed columns
# ---------------------------------------------------------------------------

def test_create_table_typed_columns(kbc, test_bucket, test_prefix):
    """CREATE TABLE with BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP columns."""
    table_name = f"{test_prefix}ddl_typed"
    ref = f'kbc."{test_bucket}".{table_name}'
    kbc.execute(f"""
        CREATE TABLE {ref} (
            id          VARCHAR,
            counter     BIGINT,
            amount      DOUBLE,
            is_active   BOOLEAN,
            created_on  DATE,
            updated_at  TIMESTAMP
        );
    """)
    try:
        tables = _table_names_in(kbc, test_bucket)
        assert table_name in tables, f"Typed table '{table_name}' not found"

        # DESCRIBE should return 6 columns
        desc = kbc.execute(f"DESCRIBE {ref};").fetchdf()
        assert len(desc) == 6, f"Expected 6 columns, got {len(desc)}"

        type_map = {r["column_name"]: r["column_type"].upper() for _, r in desc.iterrows()}
        assert "BIGINT" in type_map.get("counter", "") or "INTEGER" in type_map.get("counter", ""), \
            f"counter: expected BIGINT, got {type_map.get('counter')}"
        assert "DOUBLE" in type_map.get("amount", "") or "FLOAT" in type_map.get("amount", ""), \
            f"amount: expected DOUBLE, got {type_map.get('amount')}"
        assert "BOOLEAN" in type_map.get("is_active", ""), \
            f"is_active: expected BOOLEAN, got {type_map.get('is_active')}"
        assert "DATE" in type_map.get("created_on", ""), \
            f"created_on: expected DATE, got {type_map.get('created_on')}"
        assert "TIMESTAMP" in type_map.get("updated_at", ""), \
            f"updated_at: expected TIMESTAMP, got {type_map.get('updated_at')}"
    finally:
        try:
            kbc.execute(f"DROP TABLE {ref};")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 7. CREATE duplicate schema fails
# ---------------------------------------------------------------------------

def test_create_duplicate_schema_fails(kbc, test_bucket):
    """CREATE SCHEMA for an already-existing bucket must raise an error."""
    with pytest.raises(Exception) as exc_info:
        kbc.execute(f'CREATE SCHEMA kbc."{test_bucket}";')
    assert str(exc_info.value).strip() != "", "Error message must not be empty"


# ---------------------------------------------------------------------------
# 8. CREATE duplicate table fails
# ---------------------------------------------------------------------------

def test_create_duplicate_table_fails(kbc, test_table):
    """CREATE TABLE with the same name in the same schema must raise an error."""
    ref = kbc_table_ref(test_table["table_id"])
    bucket_id = test_table["bucket_id"]
    table_name = test_table["table_name"]
    with pytest.raises(Exception) as exc_info:
        kbc.execute(f"""
            CREATE TABLE kbc."{bucket_id}".{table_name} (
                id VARCHAR, name VARCHAR, value VARCHAR
            );
        """)
    assert str(exc_info.value).strip() != ""


# ---------------------------------------------------------------------------
# 9. DROP non-existent table fails
# ---------------------------------------------------------------------------

def test_drop_nonexistent_table_fails(kbc, test_bucket):
    """DROP TABLE for a table that doesn't exist must raise an error."""
    with pytest.raises(Exception) as exc_info:
        kbc.execute(
            f'DROP TABLE kbc."{test_bucket}".this_table_does_not_exist_xyz_abc;'
        )
    assert str(exc_info.value).strip() != ""
