#include "keboola_insert.hpp"
#include "include/keboola_table.hpp"
#include "http/importer_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

KeboolaInsert::KeboolaInsert(LogicalOperator &op,
                              TableCatalogEntry &table,
                              physical_index_t row_id_index)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION,
                       op.types,
                       op.estimated_cardinality),
      table_(table),
      row_id_index_(row_id_index) {}

// ---------------------------------------------------------------------------
// GetGlobalSinkState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSinkState> KeboolaInsert::GetGlobalSinkState(ClientContext & /*context*/) const {
    auto gstate = make_uniq<KeboolaInsertGlobalState>();

    // Get the KeboolaTableEntry to access connection and table metadata
    auto &keboola_table = table_.Cast<KeboolaTableEntry>();

    // Populate table_id and connection from the Keboola-specific table entry
    gstate->table_id   = keboola_table.GetKeboolaTableInfo().id;
    gstate->connection = keboola_table.GetConnection();

    // Build column name list from the table's logical columns
    const auto &columns = table_.GetColumns();
    for (const auto &col : columns.Logical()) {
        gstate->column_names.push_back(col.GetName());
    }

    // Write the CSV header row
    gstate->csv_builder.AddHeader(gstate->column_names);

    return std::move(gstate);
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

SinkResultType KeboolaInsert::Sink(ExecutionContext & /*context*/,
                                    DataChunk &chunk,
                                    OperatorSinkInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaInsertGlobalState>();

    // Accumulate this chunk into the CSV buffer
    gstate.csv_builder.AddChunk(chunk, gstate.column_names);
    gstate.insert_count += static_cast<int64_t>(chunk.size());

    return SinkResultType::NEED_MORE_INPUT;
}

// ---------------------------------------------------------------------------
// Finalize
// ---------------------------------------------------------------------------

SinkFinalizeType KeboolaInsert::Finalize(Pipeline & /*pipeline*/,
                                          Event & /*event*/,
                                          ClientContext & /*context*/,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaInsertGlobalState>();

    if (gstate.insert_count == 0) {
        // Nothing to upload
        return SinkFinalizeType::READY;
    }

    const auto &conn = *gstate.connection;

    // Upload the accumulated CSV to Keboola Storage Importer
    ImporterClient importer(conn.service_urls.importer_url, conn.token);
    try {
        importer.WriteTable(gstate.table_id, gstate.csv_builder.GetCsv(), /*incremental=*/true);
    } catch (const std::exception &e) {
        throw IOException("Keboola INSERT failed for table '%s': %s",
                          gstate.table_id, std::string(e.what()));
    }

    return SinkFinalizeType::READY;
}

// ---------------------------------------------------------------------------
// GetGlobalSourceState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSourceState> KeboolaInsert::GetGlobalSourceState(ClientContext & /*context*/) const {
    return make_uniq<KeboolaInsertSourceState>();
}

// ---------------------------------------------------------------------------
// GetData — emit a single row with the insert count
// ---------------------------------------------------------------------------

SourceResultType KeboolaInsert::GetData(ExecutionContext & /*context*/,
                                         DataChunk &chunk,
                                         OperatorSourceInput &input) const {
    auto &source_state = input.global_state.Cast<KeboolaInsertSourceState>();

    if (source_state.finished) {
        return SourceResultType::FINISHED;
    }

    // Retrieve the insert count from the sink state
    auto &gstate = sink_state->Cast<KeboolaInsertGlobalState>();

    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));

    source_state.finished = true;
    return SourceResultType::FINISHED;
}

} // namespace duckdb
