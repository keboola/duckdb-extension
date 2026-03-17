"""
02 — Read Queries with Filter & Projection Pushdown
=====================================================

Demonstrates:
- Filter pushdown: WHERE, AND/OR, IN, BETWEEN, IS NULL / IS NOT NULL
- Projection pushdown: selecting only specific columns
- Aggregations: COUNT, SUM, AVG
- GROUP BY, ORDER BY, LIMIT

All pushed-down filters are translated to Keboola Query Service SQL,
so only the matching rows travel over the network.

Prerequisites:
    pip install duckdb

Before running, replace placeholder values and schema/table names
to match tables that exist in your Keboola project.
"""

import os
import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEBOOLA_TOKEN = os.environ.get("KEBOOLA_TOKEN", "YOUR_KEBOOLA_TOKEN")
KEBOOLA_URL = os.environ.get("KEBOOLA_URL", "https://connection.keboola.com")

# Replace these with real bucket/table names from your project
SCHEMA = "in.c-crm"
TABLE = "contacts"


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

    tbl = f'kbc."{SCHEMA}".{TABLE}'

    # ------------------------------------------------------------------
    # 1. Projection pushdown — only the listed columns are fetched
    # ------------------------------------------------------------------
    print("=== Projection pushdown (id, name only) ===")
    rows = con.execute(f"SELECT id, name FROM {tbl} LIMIT 5;").fetchall()
    for r in rows:
        print(f"  id={r[0]}, name={r[1]}")
    print()

    # ------------------------------------------------------------------
    # 2. Filter pushdown — equality
    # ------------------------------------------------------------------
    print("=== WHERE equality ===")
    rows = con.execute(f"""
        SELECT * FROM {tbl} WHERE status = 'active' LIMIT 5;
    """).fetchall()
    print(f"  Rows with status='active': {len(rows)}")
    print()

    # ------------------------------------------------------------------
    # 3. Filter pushdown — AND / OR
    # ------------------------------------------------------------------
    print("=== WHERE with AND ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        WHERE status = 'active' AND country = 'US'
        LIMIT 5;
    """).fetchall()
    print(f"  Active US contacts: {len(rows)}")
    print()

    print("=== WHERE with OR ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        WHERE country = 'US' OR country = 'UK'
        LIMIT 5;
    """).fetchall()
    print(f"  US or UK contacts: {len(rows)}")
    print()

    # ------------------------------------------------------------------
    # 4. Filter pushdown — IN list
    # ------------------------------------------------------------------
    print("=== WHERE IN ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        WHERE id IN ('1', '2', '3')
        ORDER BY id;
    """).fetchall()
    for r in rows:
        print(f"  id={r[0]}, name={r[1]}")
    print()

    # ------------------------------------------------------------------
    # 5. Filter pushdown — BETWEEN (range)
    # ------------------------------------------------------------------
    print("=== WHERE BETWEEN ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        WHERE id BETWEEN '10' AND '20'
        ORDER BY id;
    """).fetchall()
    print(f"  Rows with id BETWEEN '10' AND '20': {len(rows)}")
    print()

    # ------------------------------------------------------------------
    # 6. Filter pushdown — IS NULL / IS NOT NULL
    # ------------------------------------------------------------------
    print("=== WHERE IS NULL ===")
    rows = con.execute(f"""
        SELECT id FROM {tbl} WHERE email IS NULL LIMIT 5;
    """).fetchall()
    print(f"  Rows with NULL email: {len(rows)}")
    print()

    print("=== WHERE IS NOT NULL ===")
    rows = con.execute(f"""
        SELECT id FROM {tbl} WHERE email IS NOT NULL LIMIT 5;
    """).fetchall()
    print(f"  Rows with non-NULL email: {len(rows)}")
    print()

    # ------------------------------------------------------------------
    # 7. Comparison operators (>, >=, <, <=)
    # ------------------------------------------------------------------
    print("=== WHERE with comparison operators ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        WHERE id > '5'
        ORDER BY id
        LIMIT 5;
    """).fetchall()
    print(f"  Rows with id > '5': {len(rows)}")
    print()

    # ------------------------------------------------------------------
    # 8. Aggregation — COUNT, SUM, AVG
    # ------------------------------------------------------------------
    print("=== Aggregation: COUNT ===")
    total = con.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
    print(f"  Total rows: {total}")
    print()

    # ------------------------------------------------------------------
    # 9. GROUP BY
    # ------------------------------------------------------------------
    print("=== GROUP BY status ===")
    groups = con.execute(f"""
        SELECT status, COUNT(*) AS cnt
        FROM {tbl}
        GROUP BY status
        ORDER BY cnt DESC;
    """).fetchall()
    for g in groups:
        print(f"  status={g[0]}, count={g[1]}")
    print()

    # ------------------------------------------------------------------
    # 10. ORDER BY + LIMIT
    # ------------------------------------------------------------------
    print("=== ORDER BY id DESC LIMIT 3 ===")
    rows = con.execute(f"""
        SELECT id, name FROM {tbl}
        ORDER BY id DESC
        LIMIT 3;
    """).fetchall()
    for r in rows:
        print(f"  id={r[0]}, name={r[1]}")
    print()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    con.execute("DETACH kbc;")
    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
