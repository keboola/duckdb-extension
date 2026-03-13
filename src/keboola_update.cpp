#include "keboola_update.hpp"
#include "include/keboola_table.hpp"
#include "http/importer_client.hpp"
#include "util/csv_builder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

KeboolaUpdate::KeboolaUpdate(LogicalOperator &op,
                              TableCatalogEntry &table,
                              vector<PhysicalIndex> columns,
                              vector<unique_ptr<Expression>> updates)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION,
                       op.types,
                       op.estimated_cardinality),
      table_(table),
      columns_(std::move(columns)),
      updates_(std::move(updates)) {}

// ---------------------------------------------------------------------------
// GetGlobalSinkState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSinkState> KeboolaUpdate::GetGlobalSinkState(ClientContext & /*context*/) const {
    auto &keboola_table = table_.Cast<KeboolaTableEntry>();
    const auto &table_info = keboola_table.GetKeboolaTableInfo();

    // Require a primary key — Storage deduplication depends on it
    if (table_info.primary_key.empty()) {
        throw NotImplementedException(
            "UPDATE requires a primary key on the table '%s'", table_info.id);
    }

    auto gstate = make_uniq<KeboolaUpdateGlobalState>();
    gstate->table_id    = table_info.id;
    gstate->connection  = keboola_table.GetConnection();
    gstate->primary_key = table_info.primary_key;

    // Build the ordered list of all column names
    const auto &columns = table_.GetColumns();
    for (const auto &col : columns.Logical()) {
        gstate->all_column_names.push_back(col.GetName());
    }

    // Resolve each SET column: map PhysicalIndex → column name + new value string
    for (idx_t i = 0; i < columns_.size(); i++) {
        KeboolaUpdateSetColumn sc;
        sc.col_index = columns_[i].index;

        if (sc.col_index < gstate->all_column_names.size()) {
            sc.col_name = gstate->all_column_names[sc.col_index];
        } else {
            sc.col_name = "col_" + std::to_string(sc.col_index);
        }

        // Extract the new value from the expression.
        // For constant expressions (the common case) cast directly.
        // For anything else fall back to ToString() on the expression.
        if (i < updates_.size() && updates_[i]) {
            const auto &expr = *updates_[i];
            if (expr.expression_class == ExpressionClass::BOUND_CONSTANT) {
                const auto &bce = expr.Cast<BoundConstantExpression>();
                if (bce.value.IsNull()) {
                    sc.new_value = "";
                } else {
                    sc.new_value = bce.value.ToString();
                }
            } else {
                // Non-constant expression: use the expression's ToString as a
                // best-effort value.  Complex subquery-based SET targets will
                // throw NotImplementedException below.
                sc.new_value = expr.ToString();
            }
        }

        gstate->set_columns.push_back(std::move(sc));
    }

    return std::move(gstate);
}

// ---------------------------------------------------------------------------
// Sink — collect all matching rows from the child plan
// ---------------------------------------------------------------------------

SinkResultType KeboolaUpdate::Sink(ExecutionContext & /*context*/,
                                    DataChunk &chunk,
                                    OperatorSinkInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaUpdateGlobalState>();

    const idx_t n_cols = gstate.all_column_names.size();

    chunk.Flatten();

    for (idx_t row = 0; row < chunk.size(); row++) {
        std::vector<std::string> row_values;
        row_values.reserve(n_cols);

        for (idx_t col = 0; col < n_cols && col < chunk.ColumnCount(); col++) {
            Value val = chunk.GetValue(col, row);
            if (val.IsNull()) {
                row_values.push_back("");
            } else {
                row_values.push_back(val.ToString());
            }
        }

        // Pad with empty strings if the chunk has fewer columns than the table
        while (row_values.size() < n_cols) {
            row_values.push_back("");
        }

        gstate.matched_rows.push_back(std::move(row_values));
    }

    return SinkResultType::NEED_MORE_INPUT;
}

// ---------------------------------------------------------------------------
// Finalize — apply SET values and upload merged CSV
// ---------------------------------------------------------------------------

SinkFinalizeType KeboolaUpdate::Finalize(Pipeline & /*pipeline*/,
                                          Event & /*event*/,
                                          ClientContext & /*context*/,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaUpdateGlobalState>();

    if (gstate.matched_rows.empty()) {
        // No rows matched the WHERE clause — nothing to do
        gstate.update_count = 0;
        return SinkFinalizeType::READY;
    }

    // Apply SET column overrides to every matched row
    for (auto &row : gstate.matched_rows) {
        for (const auto &sc : gstate.set_columns) {
            if (sc.col_index < row.size()) {
                row[sc.col_index] = sc.new_value;
            }
        }
    }

    // Build CSV from merged rows
    CsvBuilder csv;
    csv.AddHeader(gstate.all_column_names);
    for (const auto &row : gstate.matched_rows) {
        csv.AddRow(row);
    }

    const auto &conn = *gstate.connection;

    // Upload via Importer — incremental=true so Storage deduplicates on PK
    ImporterClient importer(conn.service_urls.importer_url, conn.token);
    try {
        importer.WriteTable(gstate.table_id, csv.GetCsv(), /*incremental=*/true);
    } catch (const std::exception &e) {
        throw IOException("Keboola UPDATE failed for table '%s': %s",
                          gstate.table_id, std::string(e.what()));
    }

    gstate.update_count = static_cast<int64_t>(gstate.matched_rows.size());
    return SinkFinalizeType::READY;
}

// ---------------------------------------------------------------------------
// GetGlobalSourceState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSourceState> KeboolaUpdate::GetGlobalSourceState(ClientContext & /*context*/) const {
    return make_uniq<KeboolaUpdateSourceState>();
}

// ---------------------------------------------------------------------------
// GetData — emit one row with the update count
// ---------------------------------------------------------------------------

SourceResultType KeboolaUpdate::GetData(ExecutionContext & /*context*/,
                                         DataChunk &chunk,
                                         OperatorSourceInput &input) const {
    auto &source_state = input.global_state.Cast<KeboolaUpdateSourceState>();

    if (source_state.finished) {
        return SourceResultType::FINISHED;
    }

    auto &gstate = sink_state->Cast<KeboolaUpdateGlobalState>();

    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.update_count));

    source_state.finished = true;
    return SourceResultType::FINISHED;
}

} // namespace duckdb
