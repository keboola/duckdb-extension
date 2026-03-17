"""
01 — Quick Start with Keboola DuckDB Extension
================================================

Demonstrates:
- Installing and loading the extension
- Attaching Keboola Storage with an inline token
- Attaching Keboola Storage via a named secret (recommended)
- Basic catalog exploration: SHOW SCHEMAS, SHOW TABLES, DESCRIBE
- Running a simple SELECT query

Prerequisites:
    pip install duckdb

Before running, replace the placeholder values:
    YOUR_KEBOOLA_TOKEN  → your Keboola Storage API token
    KEBOOLA_STACK_URL   → your stack URL (see list below)

Stack URLs:
    US (AWS)           https://connection.keboola.com
    EU (AWS)           https://connection.eu-central-1.keboola.com
    Azure North Europe https://connection.north-europe.azure.keboola.com
    GCP US             https://connection.us-east4.gcp.keboola.com
    GCP EU             https://connection.europe-west3.gcp.keboola.com
"""

import os
import duckdb

# ---------------------------------------------------------------------------
# Configuration — replace with your own values or set environment variables
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")


def main():
    con = duckdb.connect(":memory:")

    # ------------------------------------------------------------------
    # 1. Install and load the extension from DuckDB Community Extensions
    # ------------------------------------------------------------------
    print("=== Installing and loading the Keboola extension ===")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")
    print("Extension loaded successfully.\n")

    # ------------------------------------------------------------------
    # 2a. ATTACH with an inline token (simplest approach)
    # ------------------------------------------------------------------
    print("=== Attaching Keboola Storage (inline token) ===")
    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE keboola,
            TOKEN '{KEBOOLA_TOKEN}'
        );
    """)
    print("Attached as 'kbc'.\n")

    # ------------------------------------------------------------------
    # 3. Explore the catalog
    # ------------------------------------------------------------------

    # List all schemas (Keboola buckets)
    print("=== SHOW SCHEMAS ===")
    schemas = con.execute("SHOW SCHEMAS FROM kbc;").fetchall()
    for row in schemas:
        print(f"  {row[0]}")
    print()

    # List tables in a specific schema (bucket)
    # Replace 'in.c-crm' with an actual bucket in your project
    EXAMPLE_SCHEMA = "in.c-crm"
    print(f'=== SHOW TABLES FROM kbc."{EXAMPLE_SCHEMA}" ===')
    try:
        tables = con.execute(f'SHOW TABLES FROM kbc."{EXAMPLE_SCHEMA}";').fetchall()
        for row in tables:
            print(f"  {row[0]}")
    except Exception as e:
        print(f"  (schema not found — replace EXAMPLE_SCHEMA with a real bucket: {e})")
    print()

    # Describe a specific table's columns and types
    EXAMPLE_TABLE = "contacts"
    print(f'=== DESCRIBE kbc."{EXAMPLE_SCHEMA}".{EXAMPLE_TABLE} ===')
    try:
        cols = con.execute(f'DESCRIBE kbc."{EXAMPLE_SCHEMA}".{EXAMPLE_TABLE};').fetchall()
        for col in cols:
            print(f"  {col[0]:20s} {col[1]}")
    except Exception as e:
        print(f"  (table not found — replace EXAMPLE_TABLE with a real table: {e})")
    print()

    # ------------------------------------------------------------------
    # 4. Simple SELECT query
    # ------------------------------------------------------------------
    print("=== SELECT (first 5 rows) ===")
    try:
        rows = con.execute(f"""
            SELECT * FROM kbc."{EXAMPLE_SCHEMA}".{EXAMPLE_TABLE} LIMIT 5;
        """).fetchall()
        for row in rows:
            print(f"  {row}")
    except Exception as e:
        print(f"  (query failed — adjust schema/table names: {e})")
    print()

    # ------------------------------------------------------------------
    # 5. Detach and re-attach using a named secret (recommended)
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")

    print("=== Attaching via a named secret ===")
    con.execute(f"""
        CREATE SECRET my_kbc (
            TYPE keboola,
            TOKEN '{KEBOOLA_TOKEN}',
            URL   '{KEBOOLA_URL}'
        );
    """)
    con.execute("ATTACH '' AS kbc (TYPE keboola, SECRET my_kbc);")
    print("Attached as 'kbc' using secret 'my_kbc'.\n")

    # Verify the connection still works
    print("=== Verify: SHOW SCHEMAS (via secret) ===")
    schemas = con.execute("SHOW SCHEMAS FROM kbc;").fetchall()
    for row in schemas:
        print(f"  {row[0]}")
    print()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")
    con.close()
    print("Done. Connection closed.")


if __name__ == "__main__":
    main()
