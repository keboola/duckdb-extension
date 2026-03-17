"""
05 — Cross-Source Joins: Keboola + Local Files
================================================

Demonstrates:
- Attaching Keboola Storage alongside local CSV and Parquet files
- JOINing Keboola tables with local data
- Enriching Keboola data with external reference files
- Writing JOIN results back to Keboola Storage

DuckDB's multi-source capability lets you combine Keboola data with any
local or remote data source in a single SQL query — no ETL pipeline needed.

Prerequisites:
    pip install duckdb

Before running, replace placeholder values and schema/table names.
This example creates temporary local CSV/Parquet files for demonstration.
"""

import os
import csv
import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")

# Replace with real bucket/table names from your project
SOURCE_SCHEMA = "in.c-sales"
SOURCE_TABLE = "orders"
TARGET_SCHEMA = "out.c-analytics"
TARGET_TABLE = "enriched_orders"


def create_sample_csv(path: str) -> None:
    """Create a sample CSV file with customer segments for the JOIN demo."""
    rows = [
        {"customer_id": "1", "segment": "enterprise", "region": "US"},
        {"customer_id": "2", "segment": "smb",        "region": "EU"},
        {"customer_id": "3", "segment": "enterprise", "region": "US"},
        {"customer_id": "4", "segment": "startup",    "region": "APAC"},
        {"customer_id": "5", "segment": "smb",        "region": "EU"},
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "segment", "region"])
        writer.writeheader()
        writer.writerows(rows)


def create_sample_parquet(con: duckdb.DuckDBPyConnection, path: str) -> None:
    """Create a sample Parquet file with product categories."""
    con.execute(f"""
        COPY (
            SELECT *
            FROM (VALUES
                ('P001', 'Electronics', 0.10),
                ('P002', 'Apparel',     0.05),
                ('P003', 'Electronics', 0.10),
                ('P004', 'Home',        0.08)
            ) AS t(product_id, category, discount_rate)
        ) TO '{path}' (FORMAT PARQUET);
    """)


def main():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL keboola FROM community;")
    con.execute("LOAD keboola;")

    # ------------------------------------------------------------------
    # 1. Attach Keboola Storage
    # ------------------------------------------------------------------
    print("=== Attaching Keboola Storage ===")
    con.execute(f"""
        ATTACH '{KEBOOLA_URL}' AS kbc (
            TYPE keboola,
            TOKEN '{KEBOOLA_TOKEN}'
        );
    """)
    print("  Attached.\n")

    kbc_table = f'kbc."{SOURCE_SCHEMA}".{SOURCE_TABLE}'

    # ------------------------------------------------------------------
    # 2. Create local reference files
    # ------------------------------------------------------------------
    csv_path = "/tmp/segments.csv"
    parquet_path = "/tmp/products.parquet"

    create_sample_csv(csv_path)
    create_sample_parquet(con, parquet_path)
    print("=== Created local CSV and Parquet files ===\n")

    # ------------------------------------------------------------------
    # 3. JOIN Keboola table with a local CSV
    #    DuckDB's read_csv() auto-detects column types.
    # ------------------------------------------------------------------
    print("=== JOIN Keboola orders with local CSV (segments) ===")
    rows = con.execute(f"""
        SELECT
            k.customer_id,
            k.revenue,
            s.segment,
            s.region
        FROM {kbc_table} k
        JOIN read_csv('{csv_path}') s
            ON k.customer_id = s.customer_id
        ORDER BY k.customer_id
        LIMIT 10;
    """).fetchall()
    for r in rows:
        print(f"  customer={r[0]}, revenue={r[1]}, segment={r[2]}, region={r[3]}")
    print()

    # ------------------------------------------------------------------
    # 4. JOIN Keboola table with a local Parquet file
    # ------------------------------------------------------------------
    print("=== JOIN Keboola orders with local Parquet (products) ===")
    rows = con.execute(f"""
        SELECT
            k.customer_id,
            k.product_id,
            k.revenue,
            p.category,
            p.discount_rate
        FROM {kbc_table} k
        JOIN read_parquet('{parquet_path}') p
            ON k.product_id = p.product_id
        ORDER BY k.customer_id
        LIMIT 10;
    """).fetchall()
    for r in rows:
        print(f"  customer={r[0]}, product={r[1]}, revenue={r[2]}, "
              f"category={r[3]}, discount={r[4]}")
    print()

    # ------------------------------------------------------------------
    # 5. Three-way JOIN: Keboola + CSV + Parquet
    # ------------------------------------------------------------------
    print("=== Three-way JOIN: Keboola + CSV + Parquet ===")
    rows = con.execute(f"""
        SELECT
            k.customer_id,
            s.segment,
            p.category,
            SUM(k.revenue) AS total_revenue
        FROM {kbc_table} k
        JOIN read_csv('{csv_path}') s
            ON k.customer_id = s.customer_id
        JOIN read_parquet('{parquet_path}') p
            ON k.product_id = p.product_id
        GROUP BY k.customer_id, s.segment, p.category
        ORDER BY total_revenue DESC
        LIMIT 10;
    """).fetchall()
    for r in rows:
        print(f"  customer={r[0]}, segment={r[1]}, category={r[2]}, "
              f"total_revenue={r[3]}")
    print()

    # ------------------------------------------------------------------
    # 6. INSERT JOIN results back to Keboola
    #    Write the enriched data to a Keboola output table.
    #    (The target table must already exist, or create it first.)
    # ------------------------------------------------------------------
    print("=== INSERT enriched results back to Keboola ===")
    target = f'kbc."{TARGET_SCHEMA}".{TARGET_TABLE}'
    try:
        con.execute(f"""
            INSERT INTO {target}
            SELECT
                k.customer_id,
                s.segment,
                s.region,
                SUM(k.revenue) AS total_revenue
            FROM {kbc_table} k
            JOIN read_csv('{csv_path}') s
                ON k.customer_id = s.customer_id
            GROUP BY k.customer_id, s.segment, s.region;
        """)
        print("  Results written to Keboola.\n")
    except Exception as e:
        print(f"  (Could not write — ensure the target table exists: {e})\n")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")
    con.close()

    # Remove temporary files
    for path in (csv_path, parquet_path):
        if os.path.exists(path):
            os.remove(path)

    print("Done.")


if __name__ == "__main__":
    main()
