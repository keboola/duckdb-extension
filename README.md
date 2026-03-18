# Keboola DuckDB Extension

[![CI](https://github.com/keboola/duckdb-extension/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/keboola/duckdb-extension/actions/workflows/MainDistributionPipeline.yml)
[![DuckDB](https://img.shields.io/badge/DuckDB-v1.5.0-yellow)](https://duckdb.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A native DuckDB extension that exposes [Keboola Storage](https://www.keboola.com) as a first-class DuckDB database. Query Keboola tables with standard SQL, push results back to Storage, and manage your data directly from DuckDB or any tool that embeds it (MotherDuck, Metabase, dbt, Python, R, …).

## Quick Start

```sql
-- Install and load
INSTALL keboola FROM community;
LOAD keboola;

-- Attach your Keboola project
ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE keboola,
    TOKEN 'my-storage-api-token'
);

-- Query any Keboola table
SELECT * FROM kbc."in.c-crm".contacts WHERE status = 'active' LIMIT 10;

-- Write results back
INSERT INTO kbc."out.c-analytics".summary
SELECT region, SUM(revenue) AS total
FROM kbc."in.c-sales".orders
GROUP BY region;
```

---

## ATTACH Variants

### Inline token (simplest)

```sql
ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE keboola,
    TOKEN 'my-storage-api-token'
);
```

### Using a named secret (recommended for scripts)

```sql
CREATE SECRET my_kbc (
    TYPE keboola,
    TOKEN 'my-storage-api-token',
    URL   'https://connection.keboola.com'
);

ATTACH '' AS kbc (TYPE keboola, SECRET my_kbc);
```

### SNAPSHOT mode — pull all data locally for offline / low-latency queries

```sql
ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE    keboola,
    TOKEN   'my-storage-api-token',
    SNAPSHOT true
);

-- Tables are now cached in a local DuckDB file; queries never hit the API
SELECT COUNT(*) FROM kbc."in.c-crm".contacts;
```

### Read-only mode

By default, ATTACH opens the database in **read-write** mode (INSERT, UPDATE, DELETE, and DDL are allowed).  Pass `READ_ONLY true` to restrict the connection to SELECT only:

```sql
ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE      keboola,
    TOKEN     'my-storage-api-token',
    READ_ONLY true
);
```

### Development branch

```sql
ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE   keboola,
    TOKEN  'my-storage-api-token',
    BRANCH 'my-feature-branch'
);
```

---

## Supported SQL Operations

### SELECT — filter and projection pushdown

```sql
-- Full table scan
SELECT * FROM kbc."in.c-crm".contacts;

-- Column projection (only fetches requested columns)
SELECT id, name, email FROM kbc."in.c-crm".contacts;

-- Filter pushdown (translated to Keboola Query Service where clause)
SELECT * FROM kbc."in.c-crm".contacts
WHERE status = 'active' AND country = 'US';

-- Join Keboola data with a local CSV
SELECT k.customer_id, k.revenue, l.segment
FROM kbc."in.c-sales".orders k
JOIN read_csv('segments.csv') l USING (customer_id);
```

### INSERT

```sql
-- Append rows to an existing Keboola table
INSERT INTO kbc."in.c-crm".contacts (id, name, email)
VALUES ('42', 'Alice', 'alice@acme.com');

-- Bulk-insert from a local query result
INSERT INTO kbc."out.c-analytics".summary
SELECT region, SUM(revenue) FROM kbc."in.c-sales".orders GROUP BY region;
```

### UPDATE

```sql
-- Update rows (translates to a load + merge under the hood)
UPDATE kbc."in.c-crm".contacts
SET status = 'churned'
WHERE last_order_date < '2024-01-01';
```

### DELETE

```sql
-- Delete matching rows
DELETE FROM kbc."in.c-crm".contacts WHERE status = 'test';
```

### DDL — schema and table management

```sql
-- Create a new Keboola bucket (schema)
CREATE SCHEMA kbc."out.c-results";

-- Create a new Keboola table
CREATE TABLE kbc."out.c-results".report (
    id      VARCHAR PRIMARY KEY,
    value   DOUBLE,
    created TIMESTAMP
);

-- Drop a table
DROP TABLE kbc."out.c-results".report;

-- Drop a bucket (schema)
DROP SCHEMA kbc."out.c-results";
```

---

## Utility Functions

```sql
-- Reload the catalog from Keboola Storage (picks up tables created outside DuckDB)
SELECT keboola_refresh_catalog('kbc');

-- List all tables visible through an attached database
SELECT * FROM keboola_tables('kbc');

-- Download snapshot of a specific table to a local Parquet file
SELECT keboola_pull('kbc', 'in.c-crm', 'contacts', '/tmp/contacts.parquet');

-- Current extension version
SELECT keboola_version();
```

---

## Keboola Stack URLs

| Stack | URL |
|-------|-----|
| US (AWS) | `https://connection.keboola.com` |
| EU (AWS) | `https://connection.eu-central-1.keboola.com` |
| Azure North Europe | `https://connection.north-europe.azure.keboola.com` |
| GCP US | `https://connection.us-east4.gcp.keboola.com` |
| GCP EU | `https://connection.europe-west3.gcp.keboola.com` |

---

## Type Mapping

| Keboola / Snowflake type | DuckDB type |
|--------------------------|-------------|
| `VARCHAR`, `TEXT`, `STRING` | `VARCHAR` |
| `NUMBER(p,0)`, `INTEGER`, `BIGINT` | `BIGINT` |
| `NUMBER(p,s)`, `FLOAT`, `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIMESTAMP`, `TIMESTAMP_NTZ` | `TIMESTAMP` |
| `TIMESTAMP_TZ`, `TIMESTAMP_LTZ` | `TIMESTAMPTZ` |
| `ARRAY`, `OBJECT`, `VARIANT` | `JSON` |
| `BINARY` | `BLOB` |

---

## Building from Source

**Prerequisites:** CMake >= 3.21, a C++17 compiler, Git, vcpkg (optional — handled by the build system).

```bash
# Clone with submodules (DuckDB + extension-ci-tools)
git clone --recurse-submodules https://github.com/keboola/keboola-duckdb-extension.git
cd keboola-duckdb-extension

# Build release binaries
make release

# Build debug binaries
make debug

# Clean all build artefacts
make clean
```

The compiled extension is placed at:

```
build/release/extension/keboola/keboola.duckdb_extension
```

Load it directly in DuckDB (local builds are unsigned — start DuckDB with the `-unsigned` flag):

```bash
duckdb -unsigned
```

```sql
LOAD 'build/release/extension/keboola/keboola.duckdb_extension';
```

### Installing a release binary

Download the `.duckdb_extension` file for your platform from the [GitHub Releases](https://github.com/keboola/keboola-duckdb-extension/releases) page.  The release artifact is named `keboola.duckdb_extension` (e.g. `keboola-osx_arm64.duckdb_extension`). Rename it to `keboola.duckdb_extension` if necessary, then load it with the `-unsigned` flag:

```bash
duckdb -unsigned
```

```sql
LOAD '/path/to/keboola.duckdb_extension';
```

---

## Running Tests

### Offline SQL logic tests (no credentials required)

The test suite under `test/sql/` can be run without a live Keboola account:

```bash
make test-offline
# or directly:
duckdb -unsigned < test/sql/attach.test
```

In CI these tests run automatically on every push via the `MainDistributionPipeline` workflow.

### End-to-end live tests (requires Keboola credentials)

```bash
export KEBOOLA_TOKEN=my-storage-api-token
export KEBOOLA_URL=https://connection.keboola.com

# Using uv (recommended)
uv run pytest test/e2e/ -m live --timeout=120 -v

# Or plain pytest if dependencies are already installed
pytest test/e2e/ -m live --timeout=120 -v
```

The E2E workflow (`.github/workflows/E2ETests.yml`) runs on a weekly schedule and on manual dispatch — it never triggers on ordinary pushes to avoid consuming live API credits.

### Docker-based test suite

```bash
# Offline suite only
cd test && make test-docker-offline

# Full suite (offline + live) — requires KEBOOLA_TOKEN in environment
export KEBOOLA_TOKEN=my-storage-api-token
cd test && make test-docker-all
```

---

## Contributing

1. Fork the repository and create a feature branch.
2. Write tests for any new functionality under `test/sql/` (offline) or `test/e2e/` (live).
3. Run `make release && make test-offline` locally before opening a PR.
4. Open a pull request — CI will build all 6 platforms automatically.

---

## License

[MIT](LICENSE) — Copyright (c) 2024–2026 Keboola s.r.o.
