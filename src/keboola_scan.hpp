#pragma once

#include "keboola_connection.hpp"
#include "http/storage_api_client.hpp"
#include "http/query_service_client.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaScanBindData
// ---------------------------------------------------------------------------

//! Bind data created once per scan node. Holds everything needed to execute the scan.
struct KeboolaScanBindData : public FunctionData {
    //! Shared connection state (token, URLs, workspace/branch IDs)
    std::shared_ptr<KeboolaConnection> connection;
    //! Full table metadata (id, columns, etc.)
    KeboolaTableInfo table_info;
    //! Indices of columns to fetch (projection pushdown). May be empty = all columns.
    vector<column_t> column_ids;

    //! Pointer to the owning table entry — needed so get_bind_info can expose
    //! the TableCatalogEntry to DuckDB's DELETE/UPDATE binder.
    optional_ptr<TableCatalogEntry> table_entry;

    // Phase 6: snapshot support — pre-fetched rows bypass the Query Service
    bool is_snapshot = false;
    //! Pointer to the table entry's snapshot rows (not owned — entry outlives scan)
    const std::vector<std::vector<std::string>> *snapshot_rows     = nullptr;
    const std::vector<std::vector<bool>>        *snapshot_null_mask = nullptr;

    unique_ptr<FunctionData> Copy() const override;
    bool Equals(const FunctionData &other) const override;
};

// ---------------------------------------------------------------------------
// KeboolaScanGlobalState
// ---------------------------------------------------------------------------

//! Global state shared across all threads for one scan.
//! Streams results page-by-page from the Query Service instead of
//! materializing everything in memory upfront.
struct KeboolaScanGlobalState : public GlobalTableFunctionState {
    // --- Page buffer (current page of rows from Query Service) ---
    //! Rows in the current page buffer.
    vector<vector<string>> page_rows;
    //! Null mask for the current page buffer.
    vector<vector<bool>> page_null_mask;
    //! Current read position within the page buffer.
    idx_t page_position = 0;

    // --- Streaming state (live mode) ---
    //! Query Service client for fetching subsequent pages.
    unique_ptr<QueryServiceClient> qsc;
    //! Job ID from Query Service (for fetching result pages).
    std::string job_id;
    //! Statement ID from Query Service.
    std::string statement_id;
    //! Offset for the next page to fetch from the Query Service.
    int64_t next_fetch_offset = 0;
    //! Total number of rows reported by the Query Service (-1 = unknown).
    int64_t total_rows = -1;
    //! Whether all pages have been fetched from the Query Service.
    bool all_pages_fetched = false;

    // --- Column metadata ---
    //! DuckDB types for each projected data column.
    vector<LogicalType> column_types;
    //! Maps output column index → data column index, or -1 for COLUMN_IDENTIFIER_ROW_ID.
    vector<int> data_col_map;
    //! Whether the scan is finished (no more rows to return).
    bool done = false;
    //! Mutex for page fetching (ensures only one thread fetches at a time).
    std::mutex fetch_mutex;

    KeboolaScanGlobalState() {}
    idx_t MaxThreads() const override { return 1; }

    //! Fetch the next page of results from the Query Service into the page buffer.
    //! Returns true if rows were fetched, false if no more pages.
    bool FetchNextPage();
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

//! Returns a configured TableFunction for scanning Keboola tables.
TableFunction KeboolaGetScanFunction();

} // namespace duckdb
