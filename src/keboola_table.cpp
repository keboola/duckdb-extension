#include "keboola_table.hpp"
#include "keboola_scan.hpp"
#include "http/query_service_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

KeboolaTableEntry::KeboolaTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                     CreateTableInfo &info,
                                     KeboolaTableInfo table_info,
                                     std::shared_ptr<KeboolaConnection> connection)
    : TableCatalogEntry(catalog, schema, info),
      table_info_(std::move(table_info)),
      connection_(std::move(connection)) {}

// ---------------------------------------------------------------------------
// GetScanFunction — Phase 3: real scan via Query Service
// ---------------------------------------------------------------------------

TableFunction KeboolaTableEntry::GetScanFunction(ClientContext & /*context*/,
                                                  unique_ptr<FunctionData> &bind_data) {
    auto func = KeboolaGetScanFunction();

    // Phase 6: lazy snapshot — pull this table on first scan if not yet loaded.
    if (connection_->snapshot_mode && !is_snapshot_) {
        try {
            QueryServiceClient qsc(
                connection_->service_urls.query_url,
                connection_->token,
                connection_->branch_id,
                connection_->workspace_id
            );
            // SELECT * to fetch all rows; filters are applied after local load.
            std::string sql = "SELECT * FROM \"" + table_info_.id + "\"";
            // Use the same quoting as KeboolaSqlGenerator: split on last dot.
            auto dot = table_info_.id.rfind('.');
            if (dot != std::string::npos) {
                std::string schema_part = table_info_.id.substr(0, dot);
                std::string table_part  = table_info_.id.substr(dot + 1);
                sql = "SELECT * FROM \"" + schema_part + "\".\"" + table_part + "\"";
            }
            QueryServiceResult result = qsc.ExecuteQuery(sql);
            SetSnapshotData(std::move(result.rows), std::move(result.null_mask));
        } catch (const std::exception &) {
            // If pull fails, fall through to live mode for this scan.
        }
    }

    // Pre-populate bind_data with this table's connection and metadata.
    auto data = make_uniq<KeboolaScanBindData>();
    data->connection  = connection_;
    data->table_info  = table_info_;
    data->table_entry = this;  // enables get_bind_info for DELETE/UPDATE binder

    // Phase 6: if snapshot data is available, skip live Query Service call
    if (is_snapshot_) {
        data->is_snapshot        = true;
        data->snapshot_rows      = &snapshot_rows_;
        data->snapshot_null_mask = &snapshot_null_mask_;
    }

    bind_data = std::move(data);
    return func;
}

// ---------------------------------------------------------------------------
// GetStorageInfo
// ---------------------------------------------------------------------------

TableStorageInfo KeboolaTableEntry::GetStorageInfo(ClientContext & /*context*/) {
    TableStorageInfo info;
    info.cardinality = 0;
    return info;
}

// ---------------------------------------------------------------------------
// GetStatistics
// ---------------------------------------------------------------------------

unique_ptr<BaseStatistics> KeboolaTableEntry::GetStatistics(ClientContext & /*context*/,
                                                             column_t /*column_id*/) {
    return nullptr;
}

} // namespace duckdb
