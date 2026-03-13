#include "keboola_schema.hpp"
#include "keboola_table.hpp"
#include "http/query_service_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
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
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

// ---------------------------------------------------------------------------
// Helper: convert a Keboola column DuckDB type string to LogicalType
// ---------------------------------------------------------------------------

static LogicalType StringToLogicalType(const std::string &type_str) {
    const auto t = StringUtil::Upper(type_str);
    if (t == "VARCHAR")     return LogicalType::VARCHAR;
    if (t == "BIGINT")      return LogicalType::BIGINT;
    if (t == "DOUBLE")      return LogicalType::DOUBLE;
    if (t == "BOOLEAN")     return LogicalType::BOOLEAN;
    if (t == "DATE")        return LogicalType::DATE;
    if (t == "TIMESTAMP")   return LogicalType::TIMESTAMP;
    if (t == "TIMESTAMPTZ") return LogicalType::TIMESTAMP_TZ;
    if (t == "TIME")        return LogicalType::TIME;
    if (t == "BLOB")        return LogicalType::BLOB;
    if (t == "INTEGER")     return LogicalType::INTEGER;
    if (t == "FLOAT")       return LogicalType::FLOAT;

    // DECIMAL(p,s)
    if (t.substr(0, 7) == "DECIMAL") {
        auto lp = t.find('(');
        auto rp = t.find(')');
        if (lp != std::string::npos && rp != std::string::npos) {
            auto inner = t.substr(lp + 1, rp - lp - 1);
            auto comma = inner.find(',');
            if (comma != std::string::npos) {
                try {
                    uint8_t prec = static_cast<uint8_t>(std::stoi(inner.substr(0, comma)));
                    uint8_t scale = static_cast<uint8_t>(std::stoi(inner.substr(comma + 1)));
                    return LogicalType::DECIMAL(prec, scale);
                } catch (...) {}
            }
        }
        return LogicalType::DOUBLE;
    }

    // Fallback
    return LogicalType::VARCHAR;
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

KeboolaSchemaEntry::KeboolaSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                                       KeboolaBucketInfo bucket,
                                       std::shared_ptr<KeboolaConnection> connection)
    : SchemaCatalogEntry(catalog, info),
      bucket_(std::move(bucket)),
      connection_(std::move(connection)) {
    BuildTableEntries(catalog);
}

// ---------------------------------------------------------------------------
// MakeTableEntry — build a KeboolaTableEntry from KeboolaTableInfo
// ---------------------------------------------------------------------------

unique_ptr<KeboolaTableEntry> KeboolaSchemaEntry::MakeTableEntry(
    Catalog &catalog, const KeboolaTableInfo &tbl) {

    CreateTableInfo create_info;
    create_info.catalog = catalog.GetName();
    create_info.schema  = bucket_.id;
    create_info.table   = tbl.name;

    for (auto &col : tbl.columns) {
        LogicalType dtype = StringToLogicalType(col.duckdb_type);
        ColumnDefinition cdef(col.name, dtype);
        create_info.columns.AddColumn(std::move(cdef));
    }

    return make_uniq<KeboolaTableEntry>(catalog, *this, create_info, tbl, connection_);
}

// ---------------------------------------------------------------------------
// BuildTableEntries — creates KeboolaTableEntry objects from bucket metadata
// ---------------------------------------------------------------------------

void KeboolaSchemaEntry::BuildTableEntries(Catalog &catalog) {
    for (auto &tbl : bucket_.tables) {
        auto entry = MakeTableEntry(catalog, tbl);
        std::string lower_name = StringUtil::Lower(tbl.name);
        tables_[lower_name] = std::move(entry);
    }
}

// ---------------------------------------------------------------------------
// LookupEntry
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> KeboolaSchemaEntry::LookupEntry(
    CatalogTransaction /*transaction*/,
    const EntryLookupInfo &lookup_info) {

    if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
        return nullptr;
    }

    const auto &name = StringUtil::Lower(lookup_info.GetEntryName());
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return nullptr;
    }
    return it->second.get();
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

void KeboolaSchemaEntry::Scan(ClientContext & /*context*/, CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }
    for (auto &kv : tables_) {
        callback(*kv.second);
    }
}

void KeboolaSchemaEntry::Scan(CatalogType type,
                               const std::function<void(CatalogEntry &)> &callback) {
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }
    for (auto &kv : tables_) {
        callback(*kv.second);
    }
}

// ---------------------------------------------------------------------------
// DDL: CreateTable
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateTable(
    CatalogTransaction /*transaction*/, BoundCreateTableInfo &info) {

    auto &create_info = info.base->Cast<CreateTableInfo>();

    // Handle IF NOT EXISTS: if the table already exists, return it silently
    std::string lower_name = StringUtil::Lower(create_info.table);
    auto existing = tables_.find(lower_name);
    if (existing != tables_.end()) {
        if (create_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ||
            create_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
            return existing->second.get();
        }
        throw CatalogException("Table with name \"%s\" already exists in schema \"%s\"",
                               create_info.table, bucket_.id);
    }

    // Build column definitions for the Storage API
    std::vector<std::pair<std::string, std::string>> col_defs;
    for (const auto &col : create_info.columns.Logical()) {
        col_defs.push_back({col.GetName(), col.GetType().ToString()});
    }

    // Create the table via Storage API
    KeboolaTableInfo table_info = connection_->storage_client->CreateTable(
        bucket_.id, create_info.table, col_defs);

    // Also update the local bucket_ tables list so it stays in sync
    bucket_.tables.push_back(table_info);

    // Build and store catalog entry
    auto &catalog = ParentCatalog();
    auto entry = MakeTableEntry(catalog, table_info);
    auto *ptr = entry.get();
    tables_[lower_name] = std::move(entry);

    return ptr;
}

// ---------------------------------------------------------------------------
// DDL: DropEntry
// ---------------------------------------------------------------------------

void KeboolaSchemaEntry::DropEntry(ClientContext & /*context*/, DropInfo &info) {
    if (info.type == CatalogType::TABLE_ENTRY) {
        std::string lower_name = StringUtil::Lower(info.name);
        auto it = tables_.find(lower_name);

        if (it == tables_.end()) {
            if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
                return; // IF EXISTS — silently do nothing
            }
            throw CatalogException("Table with name \"%s\" not found in schema \"%s\"",
                                   info.name, bucket_.id);
        }

        // Get the full table ID (e.g. "in.c-crm.contacts")
        const std::string &table_id = it->second->GetKeboolaTableInfo().id;

        // Drop via Storage API
        connection_->storage_client->DropTable(table_id);

        // Remove from local cache
        tables_.erase(it);

        // Also remove from bucket_.tables
        bucket_.tables.erase(
            std::remove_if(bucket_.tables.begin(), bucket_.tables.end(),
                           [&](const KeboolaTableInfo &t) {
                               return StringUtil::Lower(t.name) == lower_name;
                           }),
            bucket_.tables.end());
    }
    // Non-table DROP types (INDEX, VIEW, etc.) are not supported
}

void KeboolaSchemaEntry::Alter(CatalogTransaction /*transaction*/, AlterInfo & /*info*/) {
    throw NotImplementedException("ALTER is not yet supported in the Keboola extension");
}

// ---------------------------------------------------------------------------
// Phase 6: SNAPSHOT — PullTable / PullAllTables
// ---------------------------------------------------------------------------

void KeboolaSchemaEntry::PullTable(ClientContext & /*context*/, const std::string &table_name) {
    std::string lower_name = StringUtil::Lower(table_name);
    auto it = tables_.find(lower_name);
    if (it == tables_.end()) {
        throw CatalogException("Table \"%s\" not found in schema \"%s\"",
                               table_name, bucket_.id);
    }

    KeboolaTableEntry &entry = *it->second;
    const KeboolaTableInfo &tbl_info = entry.GetKeboolaTableInfo();

    // Build SELECT * SQL
    // Use fully-qualified name: "bucket_id"."table_name"
    // The Query Service expects DuckDB-style identifier quoting
    std::string sql = "SELECT * FROM \"" + tbl_info.id + "\"";

    QueryServiceClient qsc(
        connection_->service_urls.query_url,
        connection_->token,
        connection_->branch_id,
        connection_->workspace_id
    );

    QueryServiceResult result;
    try {
        result = qsc.ExecuteQuery(sql);
    } catch (const std::exception &e) {
        throw IOException("keboola_pull: failed to pull table '%s': %s",
                          tbl_info.id, std::string(e.what()));
    }

    entry.SetSnapshotData(std::move(result.rows), std::move(result.null_mask));
}

void KeboolaSchemaEntry::PullAllTables(ClientContext &context) {
    for (auto &kv : tables_) {
        PullTable(context, kv.second->GetKeboolaTableInfo().name);
    }
}

// ---------------------------------------------------------------------------
// Unsupported operations
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateIndex(CatalogTransaction /*transaction*/,
                                                            CreateIndexInfo & /*info*/,
                                                            TableCatalogEntry & /*table*/) {
    throw NotImplementedException("Keboola extension does not support CREATE INDEX");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateFunction(CatalogTransaction /*transaction*/,
                                                               CreateFunctionInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE FUNCTION");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateView(CatalogTransaction /*transaction*/,
                                                           CreateViewInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE VIEW");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateSequence(CatalogTransaction /*transaction*/,
                                                               CreateSequenceInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE SEQUENCE");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateTableFunction(
    CatalogTransaction /*transaction*/, CreateTableFunctionInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE TABLE FUNCTION");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateCopyFunction(
    CatalogTransaction /*transaction*/, CreateCopyFunctionInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE COPY FUNCTION");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreatePragmaFunction(
    CatalogTransaction /*transaction*/, CreatePragmaFunctionInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE PRAGMA FUNCTION");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateCollation(
    CatalogTransaction /*transaction*/, CreateCollationInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE COLLATION");
}

optional_ptr<CatalogEntry> KeboolaSchemaEntry::CreateType(CatalogTransaction /*transaction*/,
                                                           CreateTypeInfo & /*info*/) {
    throw NotImplementedException("Keboola extension does not support CREATE TYPE");
}

} // namespace duckdb
