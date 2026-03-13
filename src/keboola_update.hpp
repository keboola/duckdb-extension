#pragma once

#include "include/keboola_connection.hpp"
#include "util/csv_builder.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaUpdateGlobalState
// ---------------------------------------------------------------------------

struct KeboolaUpdateSetColumn {
    idx_t col_index;          //!< Index into the full table column list
    std::string col_name;
    std::string new_value;    //!< String representation of the new value
};

//! Global sink state for KeboolaUpdate — collects matched rows and applies SET values.
struct KeboolaUpdateGlobalState : public GlobalSinkState {
    //! All rows matching the WHERE clause, one per row, values in table-column order.
    std::vector<std::vector<std::string>> matched_rows;

    //! SET column descriptors (which columns to overwrite and with what value).
    std::vector<KeboolaUpdateSetColumn> set_columns;

    std::string table_id;
    std::vector<std::string> all_column_names;
    std::vector<std::string> primary_key;
    std::shared_ptr<KeboolaConnection> connection;

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

//! Physical operator that:
//!   1. Sinks the rows selected by the WHERE clause (provided by the child plan).
//!   2. In Finalize: overwrites SET columns with their new values and uploads
//!      the merged rows as a single incremental CSV to the Keboola Importer.
//!      Storage deduplicates on PK, so old rows are replaced.
//!   3. As a Source: emits one row with the BIGINT update count.
class KeboolaUpdate : public PhysicalOperator {
public:
    KeboolaUpdate(PhysicalPlan &physical_plan,
                  LogicalOperator &op,
                  TableCatalogEntry &table,
                  vector<PhysicalIndex> columns,
                  vector<unique_ptr<Expression>> updates);

    //! The target table entry (holds column metadata and connection).
    TableCatalogEntry &table_;
    //! Physical column indices for the SET targets.
    vector<PhysicalIndex> columns_;
    //! New-value expressions for each SET column (parallel to columns_).
    vector<unique_ptr<Expression>> updates_;

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
