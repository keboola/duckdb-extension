# DuckDB Keboola Extension — Specification

## 1. Vision

Create a DuckDB extension that turns Keboola Storage into a first-class DuckDB database. Any application — CRM, ERP, custom tool, AI-generated app — can `ATTACH` Keboola Storage and use standard SQL for both reads and writes. Reads go through Query Service API, writes go through Storage API jobs.

```sql
INSTALL keboola FROM community;
LOAD keboola;

ATTACH 'https://connection.keboola.com' AS kbc (
    TYPE keboola,
    TOKEN 'my-storage-api-token'
);

-- Read from Keboola Storage
SELECT * FROM kbc."in.c-crm".contacts WHERE status = 'active';

-- Write to Keboola Storage
INSERT INTO kbc."in.c-crm".contacts (id, name, email) VALUES ('42', 'John', 'john@acme.com');

-- Update (via incremental load with PK deduplication)
UPDATE kbc."in.c-crm".contacts SET email = 'new@acme.com' WHERE id = '42';

-- Delete rows
DELETE FROM kbc."in.c-crm".contacts WHERE status = 'archived';

-- Hybrid query: join Keboola data with local DuckDB data
SELECT c.*, l.last_login
FROM kbc."in.c-crm".contacts c
JOIN local_analytics.logins l ON c.id = l.user_id;
```

**Target users:** Keboola Data Apps, AI-generated applications, BI tools, Python/Node/Rust developers, anyone who needs SQL access to Keboola Storage.

---

## 2. Architecture

```
Application (Python, Node, Rust, CLI, Data App, ...)
    │
    └── DuckDB + keboola extension
            │
            ├── CATALOG (schema discovery)
            │   └── Storage API: GET /v2/storage/buckets, /tables
            │       → buckets become schemas, tables become tables
            │       → native types from KBC.datatype.type metadata
            │       → descriptions from KBC.description metadata
            │
            ├── SELECT (reads)
            │   └── Query Service API: POST /api/v1/branches/{b}/workspaces/{w}/queries
            │       → async job submission → poll → fetch paginated results
            │       → returns Arrow-compatible columnar data to DuckDB
            │       → filter pushdown + projection pushdown
            │
            ├── INSERT (writes)
            │   └── Storage API Importer: POST https://import.{stack}/write-table
            │       → multipart CSV upload with incremental=1
            │       → batch: collect all DataChunks → single CSV upload
            │
            ├── UPDATE (writes via incremental load + PK)
            │   └── Query Service: fetch all matching rows
            │       → merge SET values → single CSV upload (incremental=1)
            │       → Storage deduplicates on primary key
            │
            ├── DELETE
            │   └── Storage API: DELETE /v2/storage/tables/{id}/rows
            │       → deleteWhereColumn + deleteWhereValues[] filter
            │       → async job → poll for completion
            │
            └── SNAPSHOT (local cache mode)
                └── Pull all/selected data into local DuckDB on ATTACH
                    → queries run locally, no Snowflake involved
                    → keboola_pull() refreshes data on demand
```

### 2.1 Why This Approach

| Criterion | DuckDB Extension | JDBC Driver | Direct Snowflake |
|-----------|-----------------|-------------|------------------|
| Credentials needed | Keboola token only | Keboola token only | Snowflake credentials |
| Backend-agnostic | Yes (Snowflake, BigQuery, ...) | Yes | No (Snowflake only) |
| Language support | Python, Node, Rust, Go, Java, CLI, Wasm | Java/JVM only | Varies |
| Local caching | Native DuckDB feature | No | No |
| Hybrid queries | Yes (local + Keboola + any ATTACH) | No | Limited |
| Embeddable | Yes (`pip install duckdb`) | No (JVM required) | No |
| Community ecosystem | DuckDB community extensions | Custom | N/A |
| AI-friendly | `pip install duckdb` + 3 lines of SQL | JAR + Java config | Complex |

---

## 3. Extension Type & Development Approach

### 3.1 Template Choice

Use the official **DuckDB C++ Extension Template** ([duckdb/extension-template](https://github.com/duckdb/extension-template)):
- Batteries-included: build system (CMake), CI/CD (GitHub Actions), testing framework, VCPKG dependency management
- Builds for all platforms: Linux, macOS, Windows, Wasm
- Can be published as a DuckDB Community Extension

### 3.2 Extension Components

The extension implements DuckDB's **pluggable storage and transactional layer** (same approach as the PostgreSQL, MySQL, and SQLite extensions). This means Keboola Storage appears as a native DuckDB database with full SQL support.

Key DuckDB extension APIs to implement:

| API | Purpose | Reference Implementation |
|-----|---------|------------------------|
| `StorageExtension` | Register `keboola` as an ATTACH type | `postgres_scanner` |
| `Catalog` | Map Keboola buckets → schemas, tables → tables | `postgres_scanner` |
| `TableFunction` (scan) | Read rows via Query Service | `snowflake` extension |
| `TableFunction` (insert) | Write rows via Storage API Importer | `postgres_scanner` |
| `TableFunction` (update) | Update rows via incremental load | Custom |
| `TableFunction` (delete) | Delete rows via Storage API | Custom |
| `SecretType` | Register `keboola` secret type | `snowflake` extension |

### 3.3 C++ Class Structure

```cpp
// Storage extension entry point
KeboolaStorageExtension   // StorageExtension: attach_function + create_transaction_manager

// Catalog layer
KeboolaCatalog            // Catalog: LookupSchema, PlanInsert, PlanDelete, PlanUpdate
KeboolaSchemaEntry        // SchemaCatalogEntry: LookupEntry, Scan, CreateTable, DropEntry
KeboolaTableEntry         // TableCatalogEntry: GetScanFunction, GetStorageInfo

// Transaction (no-op — Keboola is non-transactional)
KeboolaTransactionManager // TransactionManager: StartTransaction, Commit, Rollback
KeboolaTransaction        // Transaction: no-op

// Physical operators (write path)
KeboolaInsert             // PhysicalOperator: Sink(DataChunk) → CSV → Importer
KeboolaDelete             // PhysicalOperator: Sink → Storage API delete-rows
KeboolaUpdate             // PhysicalOperator: Sink → fetch + merge + Importer

// Scan function (read path)
KeboolaScanFunction       // TableFunction (filter_pushdown=true): Query Service

// HTTP clients
StorageApiClient          // GET /v2/storage/*, POST /v2/storage/*, DELETE
QueryServiceClient        // POST /queries, GET /queries/{id}, GET results
ImporterClient            // POST /write-table (multipart)

// Utilities
CsvBuilder                // RFC 4180 CSV generator
PollingHelper             // Exponential backoff polling (100ms → 2s, 1.5× multiplier)
```

### 3.4 Language & Dependencies

- **Language:** C++ (for full DuckDB API access and pluggable storage layer)
- **HTTP client:** `cpp-httplib` (header-only, no VCPKG overhead)
- **JSON parser:** `yyjson` (bundled with DuckDB — zero external dependency)
- **CSV generation:** Custom minimal implementation (RFC 4180)
- **Build:** CMake + VCPKG + DuckDB extension CI toolchain

---

## 4. Detailed Design

### 4.1 Connection & Authentication

**Secret creation:**
```sql
CREATE SECRET my_keboola (
    TYPE keboola,
    TOKEN 'sapi-token-xxx',
    URL 'https://connection.keboola.com',
    BRANCH 'main'              -- optional, defaults to default branch
);
```

**ATTACH:**
```sql
-- Standard (live queries via Query Service)
ATTACH '' AS kbc (TYPE keboola, SECRET my_keboola);

-- Read-only
ATTACH '' AS kbc (TYPE keboola, SECRET my_keboola, READ_ONLY);

-- Inline credentials
ATTACH 'https://connection.keboola.com' AS kbc (TYPE keboola, TOKEN 'sapi-token-xxx');

-- Snapshot mode (pulls all data locally on ATTACH)
ATTACH '' AS kbc (TYPE keboola, SECRET my_keboola, SNAPSHOT);
```

**Connection setup flow (on ATTACH):**
1. Call `GET {url}/v2/storage` → verify token, discover services (Query Service URL, Importer URL)
2. Call `GET {url}/v2/storage/dev-branches` → resolve branch ID (default or specified)
3. Call `GET {url}/v2/storage/workspaces` → find or create workspace (tagged "duckdb-extension")
4. Build catalog from `GET {url}/v2/storage/buckets?include=tables,columns`

**Service URL discovery from Storage API index:**
```json
{
  "services": [
    {"id": "query", "url": "https://query.keboola.com"},
    {"id": "import", "url": "https://import.keboola.com"},
    {"id": "queue", "url": "https://queue.keboola.com"}
  ]
}
```

### 4.2 Catalog Mapping

Keboola Storage has a two-level hierarchy: **buckets** (contain tables) and **tables** (contain columns). This maps naturally to DuckDB's schema/table model:

| Keboola Concept | DuckDB Concept | Example |
|----------------|---------------|---------|
| Keboola project | Database (ATTACH name) | `kbc` |
| Bucket (`in.c-crm`) | Schema | `kbc."in.c-crm"` |
| Table (`contacts`) | Table | `kbc."in.c-crm".contacts` |
| Column | Column | `kbc."in.c-crm".contacts.email` |

### 4.3 Type System

**Priority order for type resolution:**
1. `KBC.datatype.type` — native backend type (Snowflake NUMBER, VARCHAR, etc.)
2. `KBC.datatype.basetype` — abstract Keboola type (STRING, INTEGER, NUMERIC, etc.)
3. Fallback: `VARCHAR`

**Native Snowflake type mapping:**

| Snowflake Native Type | DuckDB Type |
|----------------------|-------------|
| `VARCHAR` / `TEXT` | `VARCHAR` |
| `NUMBER(p,0)` / `INT` / `BIGINT` | `BIGINT` |
| `NUMBER(p,s)` where s>0 | `DECIMAL(p,s)` |
| `FLOAT` / `REAL` / `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIMESTAMP_NTZ` | `TIMESTAMP` |
| `TIMESTAMP_TZ` / `TIMESTAMP_LTZ` | `TIMESTAMPTZ` |
| `ARRAY` / `OBJECT` / `VARIANT` | `JSON` |

**Keboola basetype fallback mapping:**

| Keboola Basetype | DuckDB Type |
|-----------------|-------------|
| `STRING` (default) | `VARCHAR` |
| `INTEGER` | `BIGINT` |
| `NUMERIC` | `DOUBLE` |
| `FLOAT` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMP` |

### 4.4 Table and Column Descriptions

Keboola metadata is exposed as DuckDB comments:

```json
{
  "metadata": [{"key": "KBC.description", "value": "CRM contacts table"}],
  "columnMetadata": {
    "revenue": [{"key": "KBC.description", "value": "Total revenue in USD"}]
  }
}
```

These are loaded during catalog build and stored on `KeboolaTableEntry`. DuckDB exposes them via `SHOW CREATE TABLE` and information schema.

### 4.5 Read Path — SELECT via Query Service

**Flow:**
1. DuckDB optimizer produces a scan plan (with filters, projections, limits)
2. Extension generates SQL query for Query Service
3. Submit job: `POST /api/v1/branches/{b}/workspaces/{w}/queries`
4. Poll with exponential backoff: `GET /api/v1/queries/{jobId}` (100ms → 2s, 1.5× multiplier)
5. Fetch results page by page: `GET /api/v1/queries/{jobId}/{stmtId}/results?offset=X&pageSize=1000`
6. Stream results into DuckDB's column vectors

**Predicate pushdown:**
Push WHERE filters into the Query Service SQL. `KeboolaScanFunction` is registered with `filter_pushdown = true`. `TableFilterSet` from DuckDB is translated to SQL WHERE clause.

Supported filter types pushed down:
- `CONSTANT_COMPARISON`: `=`, `>`, `>=`, `<`, `<=`
- `IS_NULL` / `IS_NOT_NULL`
- `IN_FILTER`: `col IN (v1, v2, ...)`
- `CONJUNCTION_AND`: combined filters

**Projection pushdown:**
Only request columns that DuckDB needs (from the scan plan).

**Example:**
```
User SQL:  SELECT name, email FROM kbc."in.c-crm".contacts WHERE status = 'active' LIMIT 10
Generated: SELECT "name", "email" FROM "in.c-crm"."contacts" WHERE "status" = 'active' LIMIT 10
           → submitted to Query Service
```

### 4.6 Write Path — INSERT via Storage API Importer

**Endpoint:** `POST https://import.{stack}/write-table`

**Flow:**
1. `KeboolaCatalog::PlanInsert` → returns `KeboolaInsert` PhysicalOperator
2. `KeboolaInsert::Sink(DataChunk)` → buffer rows into in-memory CSV
3. `KeboolaInsert::Finalize` → POST multipart request (all rows in single upload)
4. `KeboolaInsert::GetData` → return inserted row count to DuckDB

**Multipart request:**
```
POST https://import.keboola.com/write-table
Headers:
  X-StorageApi-Token: {token}
Content-Type: multipart/form-data
Body:
  tableId: in.c-crm.contacts
  incremental: 1
  delimiter: ,
  enclosure: "
  data: <CSV content>
```

**Batch optimization:** All DataChunks collected before Finalize → single HTTP request.

### 4.7 Write Path — UPDATE via Incremental Load with PK

**Flow:**
1. `KeboolaCatalog::PlanUpdate` → returns `KeboolaUpdate` PhysicalOperator
2. Check: table has PK (`GET /v2/storage/tables/{tableId}` → `primaryKey[]`)
3. Fetch all matching rows via Query Service: `SELECT * FROM table WHERE pk IN (...)`
4. Merge SET values over each fetched row (complete rows required for incremental load)
5. Upload all merged rows as single CSV via Importer with `incremental=1`
6. Storage deduplicates on PK → old rows replaced

**Requirement:** Table MUST have a primary key defined.

**Error cases:**
- Table has no PK → `NOT SUPPORTED: UPDATE requires a primary key on the table`
- WHERE clause doesn't reference PK columns → `NOT SUPPORTED: UPDATE WHERE must reference primary key columns`
- No rows matched → `No rows matched the UPDATE condition`

### 4.8 Write Path — DELETE via Storage API

**Endpoint:** `DELETE https://{host}/v2/storage/tables/{tableId}/rows`

**Flow:**
1. `KeboolaCatalog::PlanDelete` → returns `KeboolaDelete` PhysicalOperator
2. Parse WHERE clause → Storage API parameters
3. Execute DELETE request
4. Poll async job for completion
5. Return affected row count

**SQL mapping:**
```sql
-- Single value
DELETE FROM kbc."in.c-crm".contacts WHERE status = 'archived'
-- → deleteWhereColumn=status, deleteWhereValues[]=archived, deleteWhereOperator=eq

-- Multiple values (IN)
DELETE FROM kbc."in.c-crm".contacts WHERE status IN ('archived', 'deleted')
-- → deleteWhereColumn=status, deleteWhereValues[]=archived, deleteWhereValues[]=deleted

-- NOT equals
DELETE FROM kbc."in.c-crm".contacts WHERE status != 'active'
-- → deleteWhereColumn=status, deleteWhereValues[]=active, deleteWhereOperator=ne

-- All rows (requires explicit confirmation parameter)
DELETE FROM kbc."in.c-crm".contacts
-- → allowTruncate=1
```

**Limitation:** Single-column WHERE only in Phase 1. Multi-column WHERE deferred to Phase 6.

### 4.9 Workspace Lifecycle

Each DuckDB session gets its own isolated workspace. This prevents one user's DETACH from breaking another user's active session on the same Keboola project.

**Workspace naming:** `duckdb-ext-{8 hex chars}` (e.g. `duckdb-ext-a3f8b21c`). Each ATTACH always creates a new workspace with a random suffix via `POST /v2/storage/workspaces`.

**Three cleanup mechanisms ensure no orphaned workspaces accumulate:**

1. **On DETACH:** `DELETE /v2/storage/workspaces/{id}` — explicit cleanup when the user disconnects.

2. **atexit handler:** Registered at ATTACH time. On graceful Python/process shutdown (`exit()`, end of script, interpreter teardown), the handler calls `DELETE /v2/storage/workspaces/{id}` even if DETACH was never called. Uses a `shared_ptr<StorageApiClient>` to keep the HTTP client alive past DuckDB's own teardown. Signal handlers (SIGINT/SIGTERM) are intentionally **not** installed to avoid overwriting the host application's handlers (e.g. Python's `KeyboardInterrupt`).

3. **Stale workspace garbage collection:** Runs at the start of each ATTACH. Scans `GET /v2/storage/workspaces` for workspaces matching the `duckdb-ext-` prefix that are older than 1 hour (based on `createdTimestamp` from the API response). These are assumed to be orphans from crashed/killed sessions and are deleted. Legacy workspaces named exactly `"duckdb-extension"` (from the old shared-workspace approach) are always deleted regardless of age.

**Lifecycle summary:**
- **ATTACH** → `CreateSessionWorkspace("duckdb-ext-{random}")` + register atexit handler + run stale GC
- **Normal operation** → workspace ID stored in `KeboolaCatalog`, reused for all Query Service calls
- **DETACH** → unregister from atexit + `DELETE /v2/storage/workspaces/{id}`
- **Graceful exit without DETACH** → atexit handler deletes workspace
- **Crash / SIGKILL** → workspace becomes orphan, cleaned up by next session's stale GC

**Concurrency:** Multiple users can safely ATTACH to the same Keboola project simultaneously. Each gets an independent workspace. DETACH by one user does not affect others.

### 4.10 SNAPSHOT Mode

For read-heavy workloads where latency matters, SNAPSHOT mode pulls all Keboola data into local DuckDB on ATTACH.

```sql
-- Full snapshot on connect — pulls everything locally
ATTACH '' AS kbc (TYPE keboola, SECRET my_secret, SNAPSHOT);

-- Refresh a specific table on demand
CALL keboola_pull('kbc."in.c-crm".contacts');

-- Refresh everything
CALL keboola_pull('kbc');
```

**Behavior:**
- After ATTACH SNAPSHOT: all SELECTs run on local DuckDB (no Snowflake, no network)
- Writes (INSERT/UPDATE/DELETE) still go through the Keboola APIs
- `keboola_pull()` = TRUNCATE local table + re-fetch from Query Service
- Workspace is created, used for pull, kept for future pulls

**Use case:** Data Apps that load data once and serve many reads.

### 4.11 DDL Support

**CREATE TABLE:**
```sql
CREATE TABLE kbc."in.c-crm".new_table (id VARCHAR, name VARCHAR, email VARCHAR);
```
→ `POST /v2/storage/buckets/in.c-crm/tables-definition`

**DROP TABLE:**
```sql
DROP TABLE kbc."in.c-crm".old_table;
```
→ `DELETE /v2/storage/tables/in.c-crm.old_table`

**CREATE SCHEMA (bucket):**
```sql
CREATE SCHEMA kbc."in.c-myapp";
```
→ `POST /v2/storage/buckets` with `stage=in`, `name=c-myapp`

### 4.12 Catalog Caching

- Catalog loaded on ATTACH and cached with **60s TTL**
- Individual table metadata (for PK checks) cached per-request
- Cache invalidated on: `DETACH` + `ATTACH`, or explicit `keboola_refresh_catalog('kbc')`

---

## 5. Project Structure

```
duckdb-keboola/
├── .github/workflows/
│   └── MainDistributionPipeline.yml
├── duckdb/                     # DuckDB submodule (pinned version)
├── extension-ci-tools/         # DuckDB CI tools submodule
├── src/
│   ├── keboola_extension.cpp   # Extension entry point, registration
│   ├── keboola_catalog.cpp     # Catalog: LookupSchema, PlanInsert/Delete/Update
│   ├── keboola_catalog.hpp
│   ├── keboola_storage.cpp     # StorageExtension (ATTACH), workspace lifecycle
│   ├── keboola_storage.hpp
│   ├── keboola_schema.cpp      # SchemaCatalogEntry: LookupEntry, CreateTable
│   ├── keboola_schema.hpp
│   ├── keboola_table.cpp       # TableCatalogEntry: GetScanFunction, type mapping
│   ├── keboola_table.hpp
│   ├── keboola_transaction.cpp # TransactionManager + Transaction (no-op)
│   ├── keboola_transaction.hpp
│   ├── keboola_scan.cpp        # KeboolaScanFunction (SELECT via Query Service)
│   ├── keboola_scan.hpp
│   ├── keboola_insert.cpp      # KeboolaInsert PhysicalOperator
│   ├── keboola_insert.hpp
│   ├── keboola_update.cpp      # KeboolaUpdate PhysicalOperator
│   ├── keboola_update.hpp
│   ├── keboola_delete.cpp      # KeboolaDelete PhysicalOperator
│   ├── keboola_delete.hpp
│   ├── keboola_secret.cpp      # SecretType registration
│   ├── keboola_secret.hpp
│   ├── keboola_snapshot.cpp    # SNAPSHOT mode + keboola_pull() function
│   ├── keboola_snapshot.hpp
│   ├── http/
│   │   ├── storage_api_client.cpp    # Storage API: buckets, tables, workspaces, delete-rows
│   │   ├── storage_api_client.hpp
│   │   ├── query_service_client.cpp  # Query Service: submit, poll, fetch results
│   │   ├── query_service_client.hpp
│   │   ├── importer_client.cpp       # Storage Importer: multipart CSV upload
│   │   ├── importer_client.hpp
│   │   └── http_client.hpp           # cpp-httplib wrapper with retry logic
│   └── util/
│       ├── csv_builder.cpp     # RFC 4180 CSV generator
│       ├── csv_builder.hpp
│       ├── type_mapper.cpp     # Snowflake/Keboola types → DuckDB types
│       ├── type_mapper.hpp
│       └── polling.hpp         # Exponential backoff polling (100ms→2s, 1.5×)
├── test/
│   └── sql/
│       ├── keboola_attach.test
│       ├── keboola_catalog.test
│       ├── keboola_select.test
│       ├── keboola_insert.test
│       ├── keboola_update.test
│       ├── keboola_delete.test
│       ├── keboola_ddl.test
│       ├── keboola_snapshot.test
│       ├── keboola_types.test
│       └── keboola_errors.test
├── CMakeLists.txt
├── Makefile
├── extension_config.cmake
├── vcpkg.json                  # cpp-httplib
└── README.md
```

---

## 6. API Reference

### 6.1 Keboola APIs Used

| Operation | Keboola API | Method | Endpoint |
|-----------|------------|--------|----------|
| Verify token + service discovery | Storage API | GET | `/v2/storage` |
| List branches | Storage API | GET | `/v2/storage/dev-branches` |
| List workspaces | Storage API | GET | `/v2/storage/workspaces` |
| Create workspace | Storage API | POST | `/v2/storage/workspaces` |
| Delete workspace | Storage API | DELETE | `/v2/storage/workspaces/{id}` |
| List buckets + tables | Storage API | GET | `/v2/storage/buckets?include=tables,columns` |
| Table detail (PK + metadata) | Storage API | GET | `/v2/storage/tables/{tableId}` |
| Create bucket | Storage API | POST | `/v2/storage/buckets` |
| Create table | Storage API | POST | `/v2/storage/buckets/{bucketId}/tables-definition` |
| Delete table | Storage API | DELETE | `/v2/storage/tables/{tableId}` |
| Submit query | Query Service | POST | `/api/v1/branches/{b}/workspaces/{w}/queries` |
| Poll query job | Query Service | GET | `/api/v1/queries/{jobId}` |
| Fetch results | Query Service | GET | `/api/v1/queries/{jobId}/{stmtId}/results?offset=X&pageSize=1000` |
| Write data | Importer | POST | `https://import.{stack}/write-table` |
| Delete rows | Storage API | DELETE | `/v2/storage/tables/{tableId}/rows` |

### 6.2 DuckDB SQL Functions Provided

```sql
-- ATTACH a Keboola project
ATTACH '' AS kbc (TYPE keboola, SECRET my_secret);
ATTACH '' AS kbc (TYPE keboola, SECRET my_secret, READ_ONLY);
ATTACH '' AS kbc (TYPE keboola, SECRET my_secret, SNAPSHOT);
ATTACH 'https://connection.keboola.com' AS kbc (TYPE keboola, TOKEN '...');

-- Secret management
CREATE SECRET name (TYPE keboola, TOKEN '...', URL '...', BRANCH '...');

-- Utility functions
SELECT keboola_version();                        -- Extension version
SELECT * FROM keboola_tables('kbc');             -- List all tables with metadata
CALL keboola_pull('kbc."in.c-crm".contacts');   -- Refresh single table (SNAPSHOT mode)
CALL keboola_pull('kbc');                        -- Refresh all tables (SNAPSHOT mode)
CALL keboola_refresh_catalog('kbc');             -- Invalidate catalog cache
```

---

## 7. Implementation Phases

### Phase 1: Project Setup & Scaffolding (1 week)

**Goal:** Repo from template, builds, loads in DuckDB.

**Deliverables:**
- Fork `duckdb/extension-template`, rename to `keboola`
- `CMakeLists.txt`: extension name, cpp-httplib via VCPKG
- `keboola_extension.cpp`: entry point, `keboola_version()` function
- GitHub Actions CI: Linux + macOS builds

**Acceptance:**
```sql
LOAD keboola;
SELECT keboola_version();  -- returns version string
```

### Phase 2: Secret + ATTACH + Catalog (3 weeks)

**Goal:** Connect to Keboola, see schemas and tables.

**Deliverables:**
- `KeboolaSecretType`: `TYPE keboola` with TOKEN, URL, BRANCH
- `KeboolaStorageExtension`: attach function, workspace lifecycle
- `KeboolaCatalog` + `KeboolaSchemaEntry` + `KeboolaTableEntry`
- `KeboolaTransactionManager` + `KeboolaTransaction` (no-op)
- `StorageApiClient`: service discovery, workspaces, buckets, tables
- Native type mapping + KBC.description loading
- Catalog cache (60s TTL)

**Acceptance:**
```sql
ATTACH '' AS kbc (TYPE keboola, SECRET my_secret);
SHOW SCHEMAS IN kbc;     -- lists buckets
SHOW TABLES IN kbc."in.c-crm";  -- lists tables
DESCRIBE kbc."in.c-crm".contacts;  -- shows columns with correct types
```

### Phase 3: SELECT via Query Service (3 weeks)

**Goal:** Read data end-to-end.

**Deliverables:**
- `KeboolaScanFunction` (filter_pushdown=true)
- `QueryServiceClient`: submit, poll, paginate
- `PollingHelper`: exponential backoff
- Predicate pushdown (=, >, <, IN, IS NULL, AND)
- Projection pushdown

**Acceptance:**
```sql
SELECT * FROM kbc."in.c-crm".contacts LIMIT 10;
SELECT name FROM kbc."in.c-crm".contacts WHERE id = '42';
SELECT c.name, l.last_login FROM kbc."in.c-crm".contacts c
  JOIN local_logins l ON c.id = l.user_id;
```

### Phase 4: INSERT via Storage Importer (2 weeks)

**Deliverables:**
- `KeboolaInsert` PhysicalOperator (Sink + Finalize + GetData)
- `CsvBuilder`: RFC 4180
- `ImporterClient`: multipart POST
- Batch: collect DataChunks → single upload

**Acceptance:**
```sql
INSERT INTO kbc."in.c-crm".contacts VALUES ('1', 'Alice', 'alice@example.com');
INSERT INTO kbc."in.c-crm".contacts SELECT * FROM read_csv('new_contacts.csv');
```

### Phase 5: UPDATE + DELETE (3 weeks)

**Deliverables:**
- `KeboolaUpdate`: PK check, batch fetch, merge, single CSV upload
- `KeboolaDelete`: parse WHERE, Storage API delete-rows, async polling
- Error handling for missing PK, unsupported WHERE

**Acceptance:**
```sql
UPDATE kbc."in.c-crm".contacts SET email = 'new@example.com' WHERE id = '42';
UPDATE kbc."in.c-crm".contacts SET status = 'vip' WHERE id IN ('1','2','3');
DELETE FROM kbc."in.c-crm".contacts WHERE status = 'archived';
```

### Phase 6: DDL + SNAPSHOT + Polish (2 weeks)

**Deliverables:**
- `CREATE TABLE`, `DROP TABLE`, `CREATE SCHEMA`
- SNAPSHOT mode in ATTACH
- `keboola_pull()` function
- `keboola_refresh_catalog()` function
- HTTP retry logic (3 attempts, exponential backoff)
- Improved error messages

### Phase 7: Community Extension & Release (2 weeks)

**Deliverables:**
- Extension descriptor YAML for `duckdb/community-extensions`
- Multi-platform CI: Linux amd64/arm64, macOS, Windows
- README + user documentation
- End-user: `INSTALL keboola FROM community; LOAD keboola;`

---

## 8. Performance Characteristics

| Operation | Expected Latency | Bottleneck |
|-----------|-----------------|------------|
| ATTACH (connection setup) | 1-3s | Service discovery + workspace resolution |
| SELECT (small result, <100 rows) | 1-3s | Query Service job submission + execution |
| SELECT (large result, >10K rows) | 3-30s | Query Service execution + pagination |
| ATTACH SNAPSHOT (1M rows) | 30-120s | Full data pull via Query Service |
| keboola_pull() (single table) | 2-30s | Depends on table size |
| INSERT (single row) | 1-3s | Importer HTTP request |
| INSERT (1000 rows, batch) | 2-5s | CSV generation + Importer |
| UPDATE (single row) | 2-5s | Fetch current row + Importer |
| UPDATE (100 rows, batch) | 3-8s | Batch fetch + Importer |
| DELETE (single condition) | 1-5s | Storage API async job |
| Catalog discovery | 0.5-2s | Storage API list buckets/tables |

---

## 9. Limitations & Constraints

| Limitation | Reason | Workaround |
|-----------|--------|------------|
| Write latency (seconds) | Storage API is batch-oriented | Optimistic UI; SNAPSHOT + keboola_pull() |
| No ACID transactions | Storage API is non-transactional | Future: buffer in extension, flush on COMMIT |
| UPDATE requires PK on table | Incremental load deduplicates on PK | Set PK via Keboola UI before using UPDATE |
| DELETE only single-column WHERE | Storage API limitation | Phase 6: multi-column via read-then-delete |
| Concurrent writes: last-write-wins | No row-level locking | Acceptable for low-frequency use case |
| Query Service requires workspace | Workspace lifecycle management | Auto-create/reuse on ATTACH |
| Catalog not live-updated | Cached 60s | keboola_refresh_catalog() or DETACH+ATTACH |
| SNAPSHOT not auto-refreshed | Pull is explicit | keboola_pull() on demand |

---

## 10. Open Questions (Decided)

| # | Question | Decision |
|---|----------|---------|
| 1 | Workspace lifecycle | Create 1 tagged "duckdb-extension", reuse, delete in destructor, cleanup orphans on ATTACH |
| 2 | Type casting | Always: native Snowflake types → DuckDB, fallback Keboola basetype, fallback VARCHAR |
| 3 | Table/column descriptions | Yes — KBC.description metadata exposed as DuckDB comments |
| 4 | Wasm/CORS | Not in scope |
| 5 | Multi-row UPDATE | Batch: fetch all matching → merge → single CSV upload |
| 6 | HTTP client | cpp-httplib (header-only) |
| 7 | JSON parser | yyjson (bundled with DuckDB) |
| 8 | SNAPSHOT refresh | keboola_pull() function; second ATTACH would fail (DETACH required) |

---

## 11. Use Case: AI-Deployed CRM on Keboola

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL keboola FROM community; LOAD keboola;")
con.execute("CREATE SECRET kbc (TYPE keboola, TOKEN ?, URL ?)", [token, url])
con.execute("ATTACH '' AS kbc (TYPE keboola, SECRET kbc)")

# Create CRM schema and tables
con.execute('CREATE SCHEMA kbc."in.c-crm"')
con.execute('''
    CREATE TABLE kbc."in.c-crm".contacts (
        id VARCHAR, name VARCHAR, email VARCHAR,
        company VARCHAR, status VARCHAR, created_at VARCHAR
    )
''')

# CRUD — standard SQL
con.execute('INSERT INTO kbc."in.c-crm".contacts VALUES (?, ?, ?, ?, ?, ?)', [...])
contacts = con.execute('SELECT * FROM kbc."in.c-crm".contacts WHERE status = \'active\'').fetchdf()
con.execute('UPDATE kbc."in.c-crm".contacts SET status = \'inactive\' WHERE id = ?', [id])
con.execute('DELETE FROM kbc."in.c-crm".contacts WHERE status = \'deleted\'')

# Snapshot for read-heavy reporting
con.execute("ATTACH '' AS kbc_snap (TYPE keboola, SECRET kbc, SNAPSHOT)")
report = con.execute('''
    SELECT c.name, c.company, a.total_revenue
    FROM kbc_snap."in.c-crm".contacts c
    JOIN kbc_snap."in.c-analytics".revenue a ON c.company = a.company_name
    ORDER BY a.total_revenue DESC
''').fetchdf()
```

---

## 12. References

- [DuckDB Extension Template](https://github.com/duckdb/extension-template) — C++ extension scaffold
- [DuckDB Community Extensions](https://duckdb.org/community_extensions/development) — Publishing guide
- [DuckDB PostgreSQL Extension](https://github.com/duckdb/duckdb-postgres) — Reference for storage extension implementation (catalog, scan, insert, transaction manager)
- [DuckDB Multi-Database Support](https://duckdb.org/2024/01/26/multi-database-support-in-duckdb.html) — Pluggable storage architecture
- [Keboola Storage API](https://keboola.docs.apiary.io/) — Storage API documentation
- [Keboola Query Service](https://developers.keboola.com/) — Query Service API
- [padak/keboola_jdbc](https://github.com/padak/keboola_jdbc) — Existing JDBC driver (reference for API patterns)
- [cpp-httplib](https://github.com/yhirose/cpp-httplib) — Header-only HTTP client
