#include "keboola_delete.hpp"
#include "include/keboola_table.hpp"
#include "http/storage_api_client.hpp"

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

KeboolaDelete::KeboolaDelete(PhysicalPlan &physical_plan,
                              LogicalOperator &op,
                              TableCatalogEntry &table,
                              KeboolaDeleteParams params)
    : PhysicalOperator(physical_plan,
                       PhysicalOperatorType::EXTENSION,
                       op.types,
                       op.estimated_cardinality),
      table_(table),
      params_(std::move(params)) {}

// ---------------------------------------------------------------------------
// GetGlobalSinkState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSinkState> KeboolaDelete::GetGlobalSinkState(ClientContext & /*context*/) const {
    auto &keboola_table = table_.Cast<KeboolaTableEntry>();

    auto gstate = make_uniq<KeboolaDeleteGlobalState>();
    gstate->table_id   = keboola_table.GetKeboolaTableInfo().id;
    gstate->connection = keboola_table.GetConnection();
    gstate->params     = params_;

    return std::move(gstate);
}

// ---------------------------------------------------------------------------
// Sink — no-op; the Storage API handles row filtering server-side
// ---------------------------------------------------------------------------

SinkResultType KeboolaDelete::Sink(ExecutionContext & /*context*/,
                                    DataChunk & /*chunk*/,
                                    OperatorSinkInput & /*input*/) const {
    return SinkResultType::NEED_MORE_INPUT;
}

// ---------------------------------------------------------------------------
// Finalize — issue the Storage API delete-rows request
// ---------------------------------------------------------------------------

SinkFinalizeType KeboolaDelete::Finalize(Pipeline & /*pipeline*/,
                                          Event & /*event*/,
                                          ClientContext & /*context*/,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaDeleteGlobalState>();

    const auto &conn = *gstate.connection;

    try {
        gstate.delete_count =
            conn.storage_client->DeleteRows(gstate.table_id, gstate.params);
    } catch (const std::exception &e) {
        throw IOException("Keboola DELETE failed for table '%s': %s",
                          gstate.table_id, std::string(e.what()));
    }

    return SinkFinalizeType::READY;
}

// ---------------------------------------------------------------------------
// GetGlobalSourceState
// ---------------------------------------------------------------------------

unique_ptr<GlobalSourceState> KeboolaDelete::GetGlobalSourceState(ClientContext & /*context*/) const {
    return make_uniq<KeboolaDeleteSourceState>();
}

// ---------------------------------------------------------------------------
// GetData — emit one row with the delete count
// ---------------------------------------------------------------------------

SourceResultType KeboolaDelete::GetDataInternal(ExecutionContext & /*context*/,
                                                 DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
    auto &source_state = input.global_state.Cast<KeboolaDeleteSourceState>();

    if (source_state.finished) {
        return SourceResultType::FINISHED;
    }

    auto &gstate = sink_state->Cast<KeboolaDeleteGlobalState>();

    chunk.SetCardinality(1);
    // Storage API delete-rows returns -1 when row count is unavailable;
    // clamp to 0 in that case so callers always see a non-negative value.
    int64_t count = gstate.delete_count >= 0 ? gstate.delete_count : 0;
    chunk.SetValue(0, 0, Value::BIGINT(count));

    source_state.finished = true;
    return SourceResultType::FINISHED;
}

} // namespace duckdb
