#pragma once

#include "keboola_connection.hpp"
#include "util/csv_builder.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaInsertGlobalState
// ---------------------------------------------------------------------------

//! Global sink state for KeboolaInsert — accumulates all rows across DataChunks.
struct KeboolaInsertGlobalState : public GlobalSinkState {
    CsvBuilder csv_builder;
    std::vector<std::string> column_names;
    int64_t insert_count = 0;
    std::string table_id;
    std::shared_ptr<KeboolaConnection> connection;
};

// ---------------------------------------------------------------------------
// KeboolaInsertSourceState
// ---------------------------------------------------------------------------

//! Global source state for KeboolaInsert — tracks whether result row was emitted.
struct KeboolaInsertSourceState : public GlobalSourceState {
    bool finished = false;
};

// ---------------------------------------------------------------------------
// KeboolaInsert PhysicalOperator
// ---------------------------------------------------------------------------

//! Physical operator that collects INSERT rows into a CSV buffer, then uploads
//! the entire buffer to Keboola Storage Importer in Finalize.
class KeboolaInsert : public PhysicalOperator {
public:
    //! Constructor for INSERT INTO.
    KeboolaInsert(LogicalOperator &op,
                  TableCatalogEntry &table,
                  physical_index_t row_id_index);

    //! The target table entry (holds column metadata and connection).
    TableCatalogEntry &table_;
    physical_index_t row_id_index_;

public:
    // -----------------------------------------------------------------------
    // Sink interface
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
    // Source interface — returns row count after finalize
    // -----------------------------------------------------------------------

    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    SourceResultType GetData(ExecutionContext &context,
                              DataChunk &chunk,
                              OperatorSourceInput &input) const override;

    bool IsSource() const override { return true; }

    // -----------------------------------------------------------------------
    // Identification
    // -----------------------------------------------------------------------

    string GetName() const override { return "KEBOOLA_INSERT"; }
};

} // namespace duckdb
