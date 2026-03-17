"""
03 — Write Operations: INSERT, UPDATE, DELETE, DDL
====================================================

Demonstrates:
- CREATE SCHEMA (Keboola bucket)
- CREATE TABLE with PRIMARY KEY
- INSERT VALUES (single and multi-row)
- INSERT FROM SELECT (bulk copy from another table)
- UPDATE with WHERE clause
- DELETE with WHERE clause
- DROP TABLE
- DROP SCHEMA

Prerequisites:
    pip install duckdb

Before running, replace placeholder values. This example creates and
destroys a temporary schema/bucket — it will NOT touch your existing data.
"""

import os
import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")

# Temporary schema for this example (will be created and dropped)
TEMP_SCHEMA = "out.c-duckdb-examples"


def main():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")

    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE keboola,
            TOKEN '{KEBOOLA_TOKEN}'
        );
    """)

    # ------------------------------------------------------------------
    # 1. CREATE SCHEMA — creates a new Keboola bucket
    # ------------------------------------------------------------------
    print("=== CREATE SCHEMA (bucket) ===")
    con.execute(f'CREATE SCHEMA kbc."{TEMP_SCHEMA}";')
    print(f'  Created schema kbc."{TEMP_SCHEMA}"\n')

    # ------------------------------------------------------------------
    # 2. CREATE TABLE with PRIMARY KEY
    # ------------------------------------------------------------------
    print("=== CREATE TABLE ===")
    con.execute(f"""
        CREATE TABLE kbc."{TEMP_SCHEMA}".users (
            id      VARCHAR PRIMARY KEY,
            name    VARCHAR,
            email   VARCHAR,
            score   DOUBLE
        );
    """)
    print(f'  Created table kbc."{TEMP_SCHEMA}".users\n')

    tbl = f'kbc."{TEMP_SCHEMA}".users'

    # ------------------------------------------------------------------
    # 3. INSERT VALUES — single row
    # ------------------------------------------------------------------
    print("=== INSERT single row ===")
    con.execute(f"""
        INSERT INTO {tbl} (id, name, email, score)
        VALUES ('1', 'Alice', 'alice@example.com', 95.5);
    """)
    print("  Inserted 1 row.\n")

    # ------------------------------------------------------------------
    # 4. INSERT VALUES — multiple rows
    # ------------------------------------------------------------------
    print("=== INSERT multiple rows ===")
    con.execute(f"""
        INSERT INTO {tbl} (id, name, email, score)
        VALUES
            ('2', 'Bob',     'bob@example.com',     82.0),
            ('3', 'Charlie', 'charlie@example.com',  91.3),
            ('4', 'Diana',   'diana@example.com',    78.9);
    """)
    print("  Inserted 3 rows.\n")

    # Verify
    count = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Total rows after INSERT: {count}\n")

    # ------------------------------------------------------------------
    # 5. INSERT FROM SELECT — bulk copy from a local DuckDB table
    # ------------------------------------------------------------------
    print("=== INSERT FROM SELECT (local → Keboola) ===")

    # Create a local staging table
    con.execute("""
        CREATE TEMP TABLE staging AS
        SELECT '5' AS id, 'Eve'   AS name, 'eve@example.com'   AS email, 88.0 AS score
        UNION ALL
        SELECT '6',       'Frank',         'frank@example.com',          73.5;
    """)

    con.execute(f"""
        INSERT INTO {tbl}
        SELECT id, name, email, score FROM staging;
    """)
    print("  Inserted 2 rows from local staging table.\n")

    count = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Total rows after bulk INSERT: {count}\n")

    # ------------------------------------------------------------------
    # 6. UPDATE with WHERE — requires PRIMARY KEY on the table
    # ------------------------------------------------------------------
    print("=== UPDATE ===")
    con.execute(f"""
        UPDATE {tbl}
        SET score = 99.9
        WHERE id = '1';
    """)
    print("  Updated Alice's score to 99.9.\n")

    # Verify the update
    row = con.execute(f"SELECT id, name, score FROM {tbl} WHERE id = '1';").fetchone()
    print(f"  After UPDATE: id={row[0]}, name={row[1]}, score={row[2]}\n")

    # ------------------------------------------------------------------
    # 7. DELETE with WHERE
    # ------------------------------------------------------------------
    print("=== DELETE ===")
    con.execute(f"DELETE FROM {tbl} WHERE id = '6';")
    print("  Deleted row with id='6'.\n")

    count = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Rows remaining after DELETE: {count}\n")

    # ------------------------------------------------------------------
    # 8. DROP TABLE
    # ------------------------------------------------------------------
    print("=== DROP TABLE ===")
    con.execute(f"DROP TABLE {tbl};")
    print(f"  Dropped table {tbl}\n")

    # ------------------------------------------------------------------
    # 9. DROP SCHEMA (bucket)
    # ------------------------------------------------------------------
    print("=== DROP SCHEMA ===")
    con.execute(f'DROP SCHEMA kbc."{TEMP_SCHEMA}";')
    print(f'  Dropped schema kbc."{TEMP_SCHEMA}"\n')

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")
    con.close()
    print("Done. All temporary resources cleaned up.")


if __name__ == "__main__":
    main()
