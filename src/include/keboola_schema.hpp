#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "keboola_connection.hpp"
#include "keboola_table.hpp"
#include "http/storage_api_client.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <chrono>

namespace duckdb {

//! Represents a Keboola bucket as a DuckDB schema.
class KeboolaSchemaEntry : public SchemaCatalogEntry {
public:
    KeboolaSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                       KeboolaBucketInfo bucket,
                       std::shared_ptr<KeboolaConnection> connection);

    //! Look up a table (or other catalog entry) within this schema.
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
                                           const EntryLookupInfo &lookup_info) override;

    //! Iterate over all entries of a given type (TABLE_ENTRY is supported).
    void Scan(ClientContext &context, CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;

    void Scan(CatalogType type,
              const std::function<void(CatalogEntry &)> &callback) override;

    // --- DDL stubs (Phase 6) ---
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
                                           BoundCreateTableInfo &info) override;

    void DropEntry(ClientContext &context, DropInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;

    // --- Unsupported operations — throw NotImplementedException ---
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                           CreateIndexInfo &info,
                                           TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction,
                                              CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction,
                                          CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction,
                                              CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction,
                                               CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction,
                                          CreateTypeInfo &info) override;

    // -----------------------------------------------------------------------
    // Phase 6: SNAPSHOT support
    // -----------------------------------------------------------------------

    //! Pull all tables in this schema into local snapshot storage.
    void PullAllTables(ClientContext &context);

    //! Pull a single table into local snapshot storage.
    //! filter: optional SQL WHERE clause (without the WHERE keyword) to filter rows.
    //! changed_since: optional ISO timestamp; only rows changed after this time are pulled.
    void PullTable(ClientContext &context, const std::string &table_name,
                   const std::string &filter = "", const std::string &changed_since = "");

    //! Expose bucket info (for keboola_tables() function).
    const KeboolaBucketInfo &GetBucketInfo() const { return bucket_; }

    //! Expose tables map (for keboola_tables() and keboola_pull()).
    const std::unordered_map<std::string, unique_ptr<KeboolaTableEntry>> &GetTables() const {
        return tables_;
    }

    //! Refresh tables_ from the Storage API for this bucket only (public for keboola_tables()).
    //! Throttled: at most one HTTP call per 2 seconds per schema entry.
    void RefreshTables();

    //! Add a table entry built externally (used by CreateTable).
    //! Transfers ownership.
    void AddTableEntry(const std::string &lower_name,
                       unique_ptr<KeboolaTableEntry> entry) {
        tables_[lower_name] = std::move(entry);
    }

private:
    KeboolaBucketInfo bucket_;
    std::shared_ptr<KeboolaConnection> connection_;
    //! Table entries keyed by table name (lower-cased for case-insensitive lookup)
    std::unordered_map<std::string, unique_ptr<KeboolaTableEntry>> tables_;

    //! Timestamp of the last RefreshTables() call (epoch = never refreshed).
    std::chrono::steady_clock::time_point last_refresh_{};

    //! Populate tables_ from bucket_ metadata (called once on construction)
    void BuildTableEntries(Catalog &catalog);

    //! Helper: build a KeboolaTableEntry from KeboolaTableInfo
    unique_ptr<KeboolaTableEntry> MakeTableEntry(Catalog &catalog,
                                                  const KeboolaTableInfo &table_info);
};

} // namespace duckdb
