#include "keboola_insert.hpp"
#include "keboola_compat.hpp"
#include "include/keboola_table.hpp"
#include "http/importer_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
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

KeboolaInsert::KeboolaInsert(PhysicalPlan &physical_plan,
                              LogicalOperator &op,
                              TableCatalogEntry &table,
                              PhysicalIndex row_id_index)
    : PhysicalOperator(physical_plan,
                       PhysicalOperatorType::EXTENSION,
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
    const auto &table_info = keboola_table.GetKeboolaTableInfo();

    // Linked buckets are owned by another project — the Storage Importer
    // API rejects writes from this side.
    if (table_info.is_linked) {
        throw NotImplementedException(
            "INSERT into linked bucket table '%s' is not supported "
            "(linked buckets are read-only — modify them in the source project)",
            table_info.id);
    }

    // Populate table_id and connection from the Keboola-specific table entry
    gstate->table_id   = table_info.id;
    gstate->connection = keboola_table.GetConnection();

    // Build column name list from the table's logical columns. The catalog
    // may have injected a "_timestamp" system column (read-only, exposed for
    // incremental sync) — exclude it from the upload: typed tables reject
    // columns outside the user-defined schema (issue #23).
    const auto &columns = table_.GetColumns();
    int64_t col_idx = 0;
    for (const auto &col : columns.Logical()) {
        std::string name = keboola_compat::NameToString(col.GetName());
        if (table_info.timestamp_injected && name == "_timestamp") {
            gstate->skip_column = col_idx;
        } else {
            gstate->column_names.push_back(std::move(name));
        }
        col_idx++;
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

    // Accumulate this chunk into the CSV buffer (skipping the injected
    // "_timestamp" column when present — see GetGlobalSinkState)
    gstate.csv_builder.AddChunk(chunk, gstate.column_names, gstate.skip_column);
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
    ImporterClient importer(conn.service_urls.importer_url, conn.service_urls.storage_url, conn.token);
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

SourceResultType KeboolaInsert::GetDataInternal(ExecutionContext & /*context*/,
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
