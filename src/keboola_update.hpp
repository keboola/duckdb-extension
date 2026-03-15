#pragma once

#include "include/keboola_connection.hpp"
#include "keboola_delete.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaUpdateSetColumn
// ---------------------------------------------------------------------------

//! Describes one SET column in an UPDATE statement.
struct KeboolaUpdateSetColumn {
    idx_t col_index;          //!< Physical column index in the table
    std::string col_name;
    std::string new_value;    //!< String representation of the new value (empty = NULL)
    bool is_null = false;     //!< true when SET col = NULL
};

// ---------------------------------------------------------------------------
// KeboolaUpdateGlobalState
// ---------------------------------------------------------------------------

//! Global sink state for KeboolaUpdate.
struct KeboolaUpdateGlobalState : public GlobalSinkState {
    std::string table_id;
    std::vector<std::string> primary_key;
    std::shared_ptr<KeboolaConnection> connection;
    std::vector<KeboolaUpdateSetColumn> set_columns;
    KeboolaDeleteParams where_params;

    int64_t update_count = 0;
};

// ---------------------------------------------------------------------------
// KeboolaUpdateSourceState
// ---------------------------------------------------------------------------

//! Global source state for KeboolaUpdate — tracks whether the result row was emitted.
struct KeboolaUpdateSourceState : public GlobalSourceState {
    bool finished = false;
};

// ---------------------------------------------------------------------------
// KeboolaUpdate PhysicalOperator
// ---------------------------------------------------------------------------

//! Physical operator that implements UPDATE for Keboola tables.
//!
//! Algorithm (same pattern as KeboolaDelete):
//!   1. Plan time: extract WHERE params (from scan table_filters) and SET params
//!      (from PhysicalProjection select_list).
//!   2. Sink: no-op — child plan chunks are ignored.
//!   3. Finalize: query the Query Service for matching rows, apply SET column
//!      overrides, re-upload as incremental CSV. Storage deduplicates on PK.
//!   4. GetDataInternal: emit one row with the BIGINT update count.
class KeboolaUpdate : public PhysicalOperator {
public:
    KeboolaUpdate(PhysicalPlan &physical_plan,
                  LogicalOperator &op,
                  TableCatalogEntry &table,
                  vector<KeboolaUpdateSetColumn> set_columns,
                  KeboolaDeleteParams where_params);

    //! The target table entry (holds column metadata and connection).
    TableCatalogEntry &table_;
    //! SET column descriptors extracted at plan time.
    vector<KeboolaUpdateSetColumn> set_columns_;
    //! WHERE parameters extracted at plan time (same as KeboolaDelete).
    KeboolaDeleteParams where_params_;

public:
    // -----------------------------------------------------------------------
    // Sink interface — no-op; actual work is done in Finalize
    // -----------------------------------------------------------------------

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

    SinkResultType Sink(ExecutionContext &context,
                        DataChunk &chunk,
                        OperatorSinkInput &input) const override;

    SinkFinalizeType Finalize(Pipeline &pipeline,
                               Event &event,
                               ClientContext &context,
                               OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override { return true; }
    bool ParallelSink() const override { return false; }

    // -----------------------------------------------------------------------
    // Source interface — returns update count after finalize
    // -----------------------------------------------------------------------

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    SourceResultType GetDataInternal(ExecutionContext &context,
                                      DataChunk &chunk,
                                      OperatorSourceInput &input) const override;

    bool IsSource() const override { return true; }

    // -----------------------------------------------------------------------
    // Identification
    // -----------------------------------------------------------------------

    string GetName() const override { return "KEBOOLA_UPDATE"; }
};

} // namespace duckdb
