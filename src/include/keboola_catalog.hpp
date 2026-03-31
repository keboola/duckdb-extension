#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "keboola_connection.hpp"
#include "keboola_schema.hpp"
#include "http/storage_api_client.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

//! Top-level catalog for an ATTACHed Keboola project.
//! Each Keboola bucket maps to a DuckDB schema; each table maps to a table entry.
class KeboolaCatalog : public Catalog {
public:
    KeboolaCatalog(AttachedDatabase &db,
                   std::shared_ptr<KeboolaConnection> connection,
                   std::vector<KeboolaBucketInfo> buckets);

    ~KeboolaCatalog() override;

    string GetCatalogType() override { return "keboola"; }

    void Initialize(bool load_builtin) override {}

    //! Look up a schema (= Keboola bucket) by name.
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                   const EntryLookupInfo &schema_lookup,
                                                   OnEntryNotFound if_not_found) override;

    //! Iterate over all schemas.
    void ScanSchemas(ClientContext &context,
                     std::function<void(SchemaCatalogEntry &)> callback) override;

    //! Schema DDL — not yet implemented.
    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
                                             CreateSchemaInfo &info) override;
    void DropSchema(ClientContext &context, DropInfo &info) override;

    //! Physical plan stubs — write operations are Phase 4/5.
    PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                         LogicalCreateTable &op,
                                         PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                  LogicalInsert &op,
                                  optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                  LogicalDelete &op,
                                  PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                  LogicalUpdate &op,
                                  PhysicalOperator &plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;

    bool InMemory() override { return false; }
    string GetDBPath() override { return connection_->url; }

    //! Called on DETACH — deletes the Keboola workspace.
    void OnDetach(ClientContext &context) override;

    // -----------------------------------------------------------------------
    // Phase 6: Catalog cache TTL and refresh
    // -----------------------------------------------------------------------

    //! Returns true if the catalog metadata is older than CATALOG_TTL_SECONDS.
    bool IsCatalogStale() const {
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
                       now - catalog_loaded_at_).count();
        return age > CATALOG_TTL_SECONDS;
    }

    //! Re-fetch bucket list from Storage API and rebuild schemas_.
    void RefreshCatalog();

    // -----------------------------------------------------------------------
    // Phase 6: SNAPSHOT / keboola_pull()
    // -----------------------------------------------------------------------

    //! Pull all tables in all schemas into local snapshot storage.
    void PullAllTables(ClientContext &context);

    //! Pull a single table into local snapshot storage.
    //! schema_name: bucket id (e.g. "in.c-crm"), table_name: table name (e.g. "contacts").
    //! filter: optional SQL WHERE clause (without the WHERE keyword).
    //! changed_since: optional ISO timestamp; only rows changed after this time are pulled.
    void PullTable(ClientContext &context,
                   const std::string &schema_name,
                   const std::string &table_name,
                   const std::string &filter = "",
                   const std::string &changed_since = "");

    //! Expose schemas map for keboola_tables() and keboola_pull() functions.
    const std::unordered_map<std::string, unique_ptr<KeboolaSchemaEntry>> &GetSchemas() const {
        return schemas_;
    }

    //! Non-const access to schemas (used by pull functions).
    std::unordered_map<std::string, unique_ptr<KeboolaSchemaEntry>> &GetSchemas() {
        return schemas_;
    }

    //! Expose connection for utility functions.
    std::shared_ptr<KeboolaConnection> GetConnection() const { return connection_; }

private:
    std::shared_ptr<KeboolaConnection> connection_;
    //! Schemas keyed by bucket id (e.g. "in.c-crm")
    std::unordered_map<std::string, unique_ptr<KeboolaSchemaEntry>> schemas_;

    std::chrono::steady_clock::time_point catalog_loaded_at_;
    static constexpr int CATALOG_TTL_SECONDS = 60;

    void BuildSchemas(std::vector<KeboolaBucketInfo> &buckets);
};

} // namespace duckdb
