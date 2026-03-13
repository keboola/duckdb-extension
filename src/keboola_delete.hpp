#pragma once

#include "include/keboola_connection.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaDeleteParams
// ---------------------------------------------------------------------------

//! Parameters derived from the DELETE WHERE clause for the Storage API delete-rows endpoint.
struct KeboolaDeleteParams {
    std::string where_column;
    std::vector<std::string> where_values;
    std::string where_operator;  //!< "eq" or "ne"
    bool allow_truncate = false; //!< true for DELETE without WHERE (truncate entire table)
};

// ---------------------------------------------------------------------------
// KeboolaDeleteGlobalState
// ---------------------------------------------------------------------------

//! Global sink state for KeboolaDelete.
struct KeboolaDeleteGlobalState : public GlobalSinkState {
    KeboolaDeleteParams params;
    std::string table_id;
    std::shared_ptr<KeboolaConnection> connection;
    int64_t delete_count = 0;
};

// ---------------------------------------------------------------------------
// KeboolaDeleteSourceState
// ---------------------------------------------------------------------------

//! Global source state for KeboolaDelete — tracks whether the result row was emitted.
struct KeboolaDeleteSourceState : public GlobalSourceState {
    bool finished = false;
};

// ---------------------------------------------------------------------------
// KeboolaDelete PhysicalOperator
// ---------------------------------------------------------------------------

//! Physical operator that translates a DELETE WHERE clause into a Storage API
//! delete-rows request and polls the resulting async job to completion.
class KeboolaDelete : public PhysicalOperator {
public:
    KeboolaDelete(LogicalOperator &op,
                  TableCatalogEntry &table,
                  KeboolaDeleteParams params);

    //! The target table entry (holds column metadata and connection).
    TableCatalogEntry &table_;
    //! WHERE clause parameters extracted at plan time.
    KeboolaDeleteParams params_;

public:
    // -----------------------------------------------------------------------
    // Sink interface — no-op; Storage API handles filtering server-side
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
    // Source interface — returns delete count after finalize
    // -----------------------------------------------------------------------

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    SourceResultType GetData(ExecutionContext &context,
                              DataChunk &chunk,
                              OperatorSourceInput &input) const override;

    bool IsSource() const override { return true; }

    // -----------------------------------------------------------------------
    // Identification
    // -----------------------------------------------------------------------

    string GetName() const override { return "KEBOOLA_DELETE"; }
};

} // namespace duckdb
