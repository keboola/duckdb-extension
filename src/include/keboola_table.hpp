#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "keboola_connection.hpp"
#include "http/storage_api_client.hpp"

#include <memory>

namespace duckdb {

//! Represents a single Keboola table in DuckDB's catalog.
class KeboolaTableEntry : public TableCatalogEntry {
public:
    KeboolaTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                      CreateTableInfo &info,
                      KeboolaTableInfo table_info,
                      std::shared_ptr<KeboolaConnection> connection);

    //! Returns a stub TableFunction — actual data scan is implemented in Phase 3.
    TableFunction GetScanFunction(ClientContext &context,
                                  unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    const KeboolaTableInfo &GetKeboolaTableInfo() const { return table_info_; }

    //! Access the shared connection (used by KeboolaInsert to reach the Importer).
    std::shared_ptr<KeboolaConnection> GetConnection() const { return connection_; }

    // -----------------------------------------------------------------------
    // Phase 6: SNAPSHOT support
    // -----------------------------------------------------------------------

    //! Returns true if this table has been pulled into local snapshot storage.
    bool IsSnapshot() const { return is_snapshot_; }

    //! Store snapshot rows (all columns, string values).
    void SetSnapshotData(std::vector<std::vector<std::string>> rows,
                         std::vector<std::vector<bool>> null_mask) {
        snapshot_rows_     = std::move(rows);
        snapshot_null_mask_ = std::move(null_mask);
        is_snapshot_       = true;
    }

    //! Access snapshot data.
    const std::vector<std::vector<std::string>> &GetSnapshotRows() const {
        return snapshot_rows_;
    }
    const std::vector<std::vector<bool>> &GetSnapshotNullMask() const {
        return snapshot_null_mask_;
    }

private:
    KeboolaTableInfo table_info_;
    std::shared_ptr<KeboolaConnection> connection_;

    // Phase 6: snapshot storage
    bool is_snapshot_ = false;
    std::vector<std::vector<std::string>> snapshot_rows_;
    std::vector<std::vector<bool>> snapshot_null_mask_;
};

} // namespace duckdb
