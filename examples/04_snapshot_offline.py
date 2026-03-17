"""
04 — SNAPSHOT Mode: Offline & Low-Latency Queries
===================================================

Demonstrates:
- Attaching in SNAPSHOT mode (pulls all data locally on ATTACH)
- Running queries against the local cache (no network round-trips)
- Using keboola_pull() to refresh a single table from Keboola
- Using keboola_pull() to export table data to a local Parquet file
- Using keboola_refresh_catalog() to pick up new tables

SNAPSHOT mode is ideal for:
- Data Apps that load data once and serve many reads
- Offline / air-gapped environments
- Low-latency analytical queries

Prerequisites:
    pip install duckdb

Before running, replace placeholder values and schema/table names.
"""

import os
import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")

# Replace with a real bucket/table in your project
SCHEMA = "in.c-crm"
TABLE = "contacts"


def main():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")

    tbl = f'kbc."{SCHEMA}".{TABLE}'

    # ------------------------------------------------------------------
    # 1. ATTACH in SNAPSHOT mode
    #    All table data is pulled into a local DuckDB cache on ATTACH.
    #    Subsequent SELECTs run entirely locally — no API calls.
    # ------------------------------------------------------------------
    print("=== ATTACH with SNAPSHOT mode ===")
    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE     keboola,
            TOKEN    '{KEBOOLA_TOKEN}',
            SNAPSHOT true
        );
    """)
    print("  Attached in SNAPSHOT mode. Data is cached locally.\n")

    # ------------------------------------------------------------------
    # 2. Query the local cache — no network round-trips
    # ------------------------------------------------------------------
    print("=== Querying local cache ===")
    count = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Row count (local): {count}")

    rows = con.execute(f"SELECT * FROM {tbl} LIMIT 5;").fetchall()
    print(f"  First 5 rows:")
    for r in rows:
        print(f"    {r}")
    print()

    # Aggregation on the local cache
    print("=== Aggregation on local cache ===")
    result = con.execute(f"""
        SELECT COUNT(*) AS total
        FROM {tbl};
    """).fetchone()
    print(f"  Total: {result[0]}\n")

    # ------------------------------------------------------------------
    # 3. keboola_pull() — refresh a single table from Keboola
    #    Useful when you know the upstream data has changed and you want
    #    to update your local snapshot without re-attaching.
    # ------------------------------------------------------------------
    print("=== keboola_pull() — refresh single table ===")
    con.execute(f"""
        SELECT keboola_pull('kbc', '{SCHEMA}', '{TABLE}', '');
    """)
    count_after = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Row count after pull: {count_after}\n")

    # ------------------------------------------------------------------
    # 4. keboola_pull() — export to a local Parquet file
    #    Downloads table data and saves it as a Parquet file for
    #    further processing with pandas, Spark, or other tools.
    # ------------------------------------------------------------------
    print("=== keboola_pull() — export to Parquet ===")
    parquet_path = "/tmp/contacts_snapshot.parquet"
    con.execute(f"""
        SELECT keboola_pull('kbc', '{SCHEMA}', '{TABLE}', '{parquet_path}');
    """)
    print(f"  Exported to {parquet_path}")

    # Verify the Parquet file is readable
    pq_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{parquet_path}');
    """).fetchone()[0]
    print(f"  Parquet file row count: {pq_count}\n")

    # ------------------------------------------------------------------
    # 5. keboola_refresh_catalog() — pick up new tables
    #    If tables were created outside DuckDB (via Keboola UI, API, or
    #    another tool), call this to refresh the schema catalog.
    # ------------------------------------------------------------------
    print("=== keboola_refresh_catalog() ===")
    con.execute("SELECT keboola_refresh_catalog('kbc');")
    print("  Catalog refreshed. New tables are now visible.\n")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")
    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
