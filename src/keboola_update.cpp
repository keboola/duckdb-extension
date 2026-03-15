#include "keboola_update.hpp"
#include "keboola_delete.hpp"
#include "include/keboola_table.hpp"
#include "http/importer_client.hpp"
#include "http/query_service_client.hpp"
#include "http/storage_api_client.hpp"
#include "util/csv_builder.hpp"
#include "util/sql_generator.hpp"

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
#include <sstream>

namespace duckdb {

// NULL sentinel: Unicode Private Use Area U+E000 (UTF-8: 0xEE 0x80 0x80).
// VARCHAR NULLs must be encoded with this sentinel so NULL is distinguishable
// from empty-string in Keboola's untyped (all-string) CSV tables.
static const char *kNullSentinel = "\xEE\x80\x80";

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

KeboolaUpdate::KeboolaUpdate(PhysicalPlan &physical_plan,
                              LogicalOperator &op,
                              TableCatalogEntry &table,
                              vector<KeboolaUpdateSetColumn> set_columns,
                              KeboolaDeleteParams where_params)
    : PhysicalOperator(physical_plan,
                       PhysicalOperatorType::EXTENSION,
                       op.types,
                       op.estimated_cardinality),
      table_(table),
      set_columns_(std::move(set_columns)),
      where_params_(std::move(where_params)) {}

// ---------------------------------------------------------------------------
// BuildWhereSql — convert KeboolaDeleteParams → SQL WHERE fragment
// ---------------------------------------------------------------------------

static std::string BuildWhereSql(const KeboolaDeleteParams &params) {
    if (params.where_column.empty() || params.where_values.empty()) {
        return "";
    }

    const std::string col = KeboolaSqlGenerator::EscapeIdentifier(params.where_column);

    if (params.where_values.size() == 1) {
        const std::string val = KeboolaSqlGenerator::EscapeStringLiteral(params.where_values[0]);
        const std::string op = (params.where_operator == "ne") ? " <> " : " = ";
        return col + op + val;
    }

    // Multiple values: emit IN list
    std::ostringstream oss;
    oss << col << " IN (";
    for (size_t i = 0; i < params.where_values.size(); i++) {
        if (i > 0) oss << ", ";
        oss << KeboolaSqlGenerator::EscapeStringLiteral(params.where_values[i]);
    }
    oss << ")";
    return oss.str();
}

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
    gstate->set_columns = set_columns_;
    gstate->where_params = where_params_;

    return std::move(gstate);
}

// ---------------------------------------------------------------------------
// Sink — no-op; actual work is done in Finalize
// ---------------------------------------------------------------------------

SinkResultType KeboolaUpdate::Sink(ExecutionContext & /*context*/,
                                    DataChunk & /*chunk*/,
                                    OperatorSinkInput & /*input*/) const {
    return SinkResultType::NEED_MORE_INPUT;
}

// ---------------------------------------------------------------------------
// Finalize — query QS for matching rows, apply SET values, upload merged CSV
// ---------------------------------------------------------------------------

SinkFinalizeType KeboolaUpdate::Finalize(Pipeline & /*pipeline*/,
                                          Event & /*event*/,
                                          ClientContext & /*context*/,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<KeboolaUpdateGlobalState>();
    const auto &conn = *gstate.connection;

    // Build the SELECT SQL to fetch matching rows from Query Service
    const std::string &table_id = gstate.table_id;

    // Split table_id on the last dot: "in.c-bucket.table" → schema + table
    std::string schema_part, table_part;
    auto last_dot = table_id.rfind('.');
    if (last_dot != std::string::npos) {
        schema_part = table_id.substr(0, last_dot);
        table_part  = table_id.substr(last_dot + 1);
    } else {
        table_part = table_id;
    }

    std::string from_clause;
    if (!schema_part.empty()) {
        from_clause = KeboolaSqlGenerator::EscapeIdentifier(schema_part) + "." +
                      KeboolaSqlGenerator::EscapeIdentifier(table_part);
    } else {
        from_clause = KeboolaSqlGenerator::EscapeIdentifier(table_part);
    }

    std::string select_sql = "SELECT * FROM " + from_clause;
    const std::string where_sql = BuildWhereSql(gstate.where_params);
    if (!where_sql.empty()) {
        select_sql += " WHERE " + where_sql;
    }

    // Execute query against the Query Service
    QueryServiceClient qsc(conn.service_urls.query_url,
                           conn.token,
                           conn.branch_id,
                           conn.workspace_id);

    QueryServiceResult result;
    try {
        result = qsc.ExecuteQuery(select_sql);
    } catch (const std::exception &e) {
        throw IOException("Keboola UPDATE: failed to fetch matching rows for table '%s': %s",
                          table_id, std::string(e.what()));
    }

    if (result.rows.empty()) {
        // No matching rows — nothing to update
        gstate.update_count = 0;
        return SinkFinalizeType::READY;
    }

    // Build a name → column-index map from the Query Service result columns
    std::vector<std::string> col_names;
    col_names.reserve(result.columns.size());
    for (const auto &col : result.columns) {
        col_names.push_back(col.name);
    }

    // Build a map from SET column name to (col_index_in_result, new_value, is_null)
    struct SetInfo { idx_t res_col_idx; std::string new_value; bool is_null; };
    std::vector<SetInfo> set_infos;
    for (const auto &sc : gstate.set_columns) {
        SetInfo si;
        si.is_null    = sc.is_null;
        si.new_value  = sc.new_value;
        si.res_col_idx = col_names.size(); // sentinel: not found

        for (idx_t ci = 0; ci < col_names.size(); ci++) {
            if (col_names[ci] == sc.col_name) {
                si.res_col_idx = ci;
                break;
            }
        }
        // If column name not found in result, skip silently (schema mismatch tolerance)
        if (si.res_col_idx == col_names.size()) {
            continue;
        }
        set_infos.push_back(si);
    }

    // Apply SET overrides to all matched rows
    auto rows = result.rows;
    auto null_mask = result.null_mask;

    for (auto &row : rows) {
        for (const auto &si : set_infos) {
            if (si.res_col_idx < row.size()) {
                row[si.res_col_idx] = si.new_value;
            }
        }
    }
    if (!null_mask.empty()) {
        for (auto &row_nulls : null_mask) {
            for (const auto &si : set_infos) {
                if (si.res_col_idx < row_nulls.size()) {
                    row_nulls[si.res_col_idx] = si.is_null;
                }
            }
        }
    }

    // Build CSV from the updated rows
    CsvBuilder csv;
    csv.AddHeader(col_names);
    for (idx_t ri = 0; ri < rows.size(); ri++) {
        const auto &row = rows[ri];
        const auto *row_nulls = (ri < null_mask.size()) ? &null_mask[ri] : nullptr;
        std::vector<std::string> csv_row;
        csv_row.reserve(row.size());
        for (idx_t ci = 0; ci < row.size(); ci++) {
            bool is_null = (row_nulls && ci < row_nulls->size() && (*row_nulls)[ci]);
            if (is_null) {
                // Use the NULL sentinel so the scanner converts this back to SQL NULL.
                // The sentinel is safe for all column types: keboola_scan.cpp checks for
                // it before any type-specific parsing and returns NULL.
                csv_row.push_back(kNullSentinel);
            } else {
                csv_row.push_back(row[ci]);
            }
        }
        csv.AddRow(csv_row);
    }

    // DELETE the matched rows by PK, then re-INSERT the updated rows.
    // This avoids relying on BigQuery's async deduplication (incremental=1 upsert),
    // which can cause stale reads immediately after the write job completes.
    // NOTE: multi-column primary keys are not supported; GetGlobalSinkState already
    // requires primary_key to be non-empty, so primary_key.size() == 1 is assumed here.
    {
        // Find PK column index in col_names
        const std::string &pk_col = gstate.primary_key[0];
        idx_t pk_col_idx = col_names.size(); // sentinel
        for (idx_t ci = 0; ci < col_names.size(); ci++) {
            if (col_names[ci] == pk_col) {
                pk_col_idx = ci;
                break;
            }
        }
        if (pk_col_idx == col_names.size()) {
            throw IOException(
                "Keboola UPDATE: primary key column '%s' not found in query result for table '%s'",
                pk_col, gstate.table_id);
        }

        // Collect PK values from the (pre-SET-override) original rows.
        // We use `result.rows` (before SET was applied) so the PK values are the
        // original keys that exist in Storage and need to be deleted.
        std::vector<std::string> pk_values;
        pk_values.reserve(result.rows.size());
        for (const auto &row : result.rows) {
            if (pk_col_idx < row.size()) {
                pk_values.push_back(row[pk_col_idx]);
            }
        }

        // Delete the existing rows from Storage by PK
        KeboolaDeleteParams delete_params;
        delete_params.where_column   = pk_col;
        delete_params.where_values   = std::move(pk_values);
        delete_params.where_operator = "eq";
        delete_params.allow_truncate = false;

        try {
            conn.storage_client->DeleteRows(gstate.table_id, delete_params);
        } catch (const std::exception &e) {
            throw IOException(
                "Keboola UPDATE: DELETE step failed for table '%s': %s",
                gstate.table_id, std::string(e.what()));
        }
    }

    // Re-INSERT the updated rows (append-only since old rows were deleted above)
    ImporterClient importer(conn.service_urls.importer_url,
                            conn.service_urls.storage_url,
                            conn.token);
    try {
        importer.WriteTable(gstate.table_id, csv.GetCsv(), /*incremental=*/true);
    } catch (const std::exception &e) {
        throw IOException("Keboola UPDATE failed for table '%s': %s",
                          gstate.table_id, std::string(e.what()));
    }

    gstate.update_count = static_cast<int64_t>(rows.size());
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

SourceResultType KeboolaUpdate::GetDataInternal(ExecutionContext & /*context*/,
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
