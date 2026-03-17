# Keboola DuckDB Extension — Examples

Tested code snippets demonstrating the main use cases of the [Keboola DuckDB Extension](../README.md).

## Prerequisites

```bash
pip install duckdb
# For the data-quality example (pytest runner):
pip install pytest
```

## Configuration

Every example reads credentials from environment variables (with fallback placeholders):

```bash
export KEBOOLA_TOKEN="your-storage-api-token"
export KEBOOLA_URL="https://connection.keboola.com"   # adjust for your stack
```

| Stack | URL |
|-------|-----|
| US (AWS) | `https://connection.keboola.com` |
| EU (AWS) | `https://connection.eu-central-1.keboola.com` |
| Azure North Europe | `https://connection.north-europe.azure.keboola.com` |
| GCP US | `https://connection.us-east4.gcp.keboola.com` |
| GCP EU | `https://connection.europe-west3.gcp.keboola.com` |

You will also need to adjust the schema (`in.c-crm`) and table (`contacts`) names in each file to match tables that exist in your Keboola project.

## Examples

| # | File | Description |
|---|------|-------------|
| 1 | [`01_quick_start.py`](01_quick_start.py) | Install, load, ATTACH (inline token & secret), explore catalog (SHOW SCHEMAS, SHOW TABLES, DESCRIBE), first SELECT |
| 2 | [`02_read_queries.py`](02_read_queries.py) | Filter pushdown (WHERE, AND/OR, IN, BETWEEN, IS NULL), projection pushdown, aggregations, GROUP BY, ORDER BY |
| 3 | [`03_write_operations.py`](03_write_operations.py) | CREATE SCHEMA, CREATE TABLE with PK, INSERT VALUES, INSERT FROM SELECT, UPDATE, DELETE, DROP TABLE, DROP SCHEMA |
| 4 | [`04_snapshot_offline.py`](04_snapshot_offline.py) | SNAPSHOT mode — local cache, offline queries, keboola_pull() refresh, Parquet export, keboola_refresh_catalog() |
| 5 | [`05_cross_source_joins.py`](05_cross_source_joins.py) | JOIN Keboola tables with local CSV/Parquet, three-way joins, INSERT results back to Keboola |
| 6 | [`06_data_quality.py`](06_data_quality.py) | Read-only validation: row counts, NULL checks, uniqueness, freshness — standalone or as pytest tests |

## Running

Each file is self-contained and runnable independently:

```bash
python examples/01_quick_start.py
python examples/02_read_queries.py
python examples/03_write_operations.py
python examples/04_snapshot_offline.py
python examples/05_cross_source_joins.py
python examples/06_data_quality.py
```

The data quality example can also be run via pytest for CI integration:

```bash
pytest examples/06_data_quality.py -v
```
