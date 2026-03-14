#pragma once

#include "keboola_connection.hpp"
#include "http/storage_api_client.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"

#include <atomic>
#include <memory>
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

//! Global state shared across all threads for one scan. Pre-fetches all rows.
struct KeboolaScanGlobalState : public GlobalTableFunctionState {
    //! All rows fetched from the Query Service (string values).
    vector<vector<string>> rows;
    //! Null mask: null_mask[row][col] == true means the cell is NULL.
    vector<vector<bool>> null_mask;
    //! DuckDB types for each projected data column (parallel to rows[][]).
    vector<LogicalType> column_types;
    //! Maps output column index → data column index, or -1 for COLUMN_IDENTIFIER_ROW_ID.
    vector<int> data_col_map;
    //! Current read position (atomic for thread safety even with MaxThreads=1).
    std::atomic<idx_t> position;
    //! Whether the scan is finished.
    bool done = false;

    KeboolaScanGlobalState() : position(0) {}

    idx_t MaxThreads() const override { return 1; }
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

//! Returns a configured TableFunction for scanning Keboola tables.
TableFunction KeboolaGetScanFunction();

} // namespace duckdb
