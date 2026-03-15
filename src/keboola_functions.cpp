#include "keboola_functions.hpp"
#include "keboola_catalog.hpp"
#include "keboola_schema.hpp"
#include "keboola_table.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// Helper: resolve a KeboolaCatalog by attached database name
// ---------------------------------------------------------------------------

static KeboolaCatalog &GetKeboolaCatalog(ClientContext &context, const std::string &db_name) {
    auto &db_manager = DatabaseManager::Get(context);
    auto attached    = db_manager.GetDatabase(context, db_name);
    if (!attached) {
        throw BinderException("Database \"%s\" is not attached", db_name);
    }
    auto &catalog = attached->GetCatalog();
    if (catalog.GetCatalogType() != "keboola") {
        throw BinderException("Database \"%s\" is not a Keboola catalog", db_name);
    }
    return catalog.Cast<KeboolaCatalog>();
}

// ---------------------------------------------------------------------------
// keboola_refresh_catalog(db_name VARCHAR) → VARCHAR
// ---------------------------------------------------------------------------

static void KeboolaRefreshCatalogFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();

    auto &input_vec = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, args.size(),
        [&](string_t db_name_str) -> string_t {
            const std::string db_name = db_name_str.GetString();
            auto &catalog = GetKeboolaCatalog(context, db_name);
            catalog.RefreshCatalog();
            const std::string msg = "Refreshed catalog for database \"" + db_name + "\"";
            return StringVector::AddString(result, msg);
        });
}

// ---------------------------------------------------------------------------
// keboola_tables(db_name VARCHAR) table function
//
// Returns: schema_name, table_name, table_id, description, primary_key
// ---------------------------------------------------------------------------

struct KeboolaTablesBindData : public FunctionData {
    std::string db_name;

    // Result rows: schema_name, table_name, table_id, description, primary_key
    std::vector<std::string> schema_names;
    std::vector<std::string> table_names;
    std::vector<std::string> table_ids;
    std::vector<std::string> descriptions;
    std::vector<std::string> primary_keys;

    unique_ptr<FunctionData> Copy() const override {
        auto copy         = make_uniq<KeboolaTablesBindData>();
        copy->db_name     = db_name;
        copy->schema_names = schema_names;
        copy->table_names  = table_names;
        copy->table_ids    = table_ids;
        copy->descriptions = descriptions;
        copy->primary_keys = primary_keys;
        return std::move(copy);
    }
    bool Equals(const FunctionData &other) const override {
        return db_name == other.Cast<KeboolaTablesBindData>().db_name;
    }
};

struct KeboolaTablesState : public GlobalTableFunctionState {
    idx_t position = 0;
};

static unique_ptr<FunctionData> KeboolaTablesBindFn(ClientContext &context,
                                                    TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names) {
    return_types = {
        LogicalType::VARCHAR,  // schema_name
        LogicalType::VARCHAR,  // table_name
        LogicalType::VARCHAR,  // table_id
        LogicalType::VARCHAR,  // description
        LogicalType::VARCHAR,  // primary_key
    };
    names = {"schema_name", "table_name", "table_id", "description", "primary_key"};

    const std::string db_name = input.inputs[0].GetValue<std::string>();

    auto &catalog = GetKeboolaCatalog(context, db_name);

    // Refresh catalog so buckets created after ATTACH are visible.
    try { catalog.RefreshCatalog(); } catch (...) {}

    auto bind_data    = make_uniq<KeboolaTablesBindData>();
    bind_data->db_name = db_name;

    for (auto &schema_kv : catalog.GetSchemas()) {
        KeboolaSchemaEntry &schema_entry = *schema_kv.second;
        const std::string &schema_name   = schema_kv.first;

        schema_entry.RefreshTables(); // pick up tables created after ATTACH (throttled)

        for (auto &tbl_kv : schema_entry.GetTables()) {
            const KeboolaTableEntry &tbl = *tbl_kv.second;
            const KeboolaTableInfo &info  = tbl.GetKeboolaTableInfo();

            bind_data->schema_names.push_back(schema_name);
            bind_data->table_names.push_back(info.name);
            bind_data->table_ids.push_back(info.id);
            bind_data->descriptions.push_back(info.description);

            // Primary key: comma-separated list
            std::string pk;
            for (size_t i = 0; i < info.primary_key.size(); ++i) {
                if (i > 0) pk += ",";
                pk += info.primary_key[i];
            }
            bind_data->primary_keys.push_back(pk);
        }
    }

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> KeboolaTablesInitGlobal(
    ClientContext & /*context*/, TableFunctionInitInput & /*input*/) {
    return make_uniq<KeboolaTablesState>();
}

static void KeboolaTablesScan(ClientContext & /*context*/, TableFunctionInput &data_p,
                               DataChunk &output) {
    auto &bind = data_p.bind_data->Cast<KeboolaTablesBindData>();
    auto &state = data_p.global_state->Cast<KeboolaTablesState>();

    idx_t count = 0;
    const idx_t total = bind.schema_names.size();

    while (state.position < total && count < STANDARD_VECTOR_SIZE) {
        idx_t i = state.position++;

        FlatVector::GetData<string_t>(output.data[0])[count] =
            StringVector::AddString(output.data[0], bind.schema_names[i]);
        FlatVector::GetData<string_t>(output.data[1])[count] =
            StringVector::AddString(output.data[1], bind.table_names[i]);
        FlatVector::GetData<string_t>(output.data[2])[count] =
            StringVector::AddString(output.data[2], bind.table_ids[i]);
        FlatVector::GetData<string_t>(output.data[3])[count] =
            StringVector::AddString(output.data[3], bind.descriptions[i]);
        FlatVector::GetData<string_t>(output.data[4])[count] =
            StringVector::AddString(output.data[4], bind.primary_keys[i]);

        count++;
    }

    output.SetCardinality(count);
}

// ---------------------------------------------------------------------------
// keboola_pull(target VARCHAR) → TABLE(status VARCHAR)
//
// target can be:
//   'kbc'                          — pull all tables in all schemas
//   'kbc."in.c-crm"'               — pull all tables in one schema
//   'kbc."in.c-crm".contacts'      — pull a single table
//
// Registered as a TableFunction so that CALL keboola_pull(...) works.
// ---------------------------------------------------------------------------

struct KeboolaPullBindData : public FunctionData {
    std::string status_msg;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<KeboolaPullBindData>();
        copy->status_msg = status_msg;
        return std::move(copy);
    }
    bool Equals(const FunctionData &other) const override {
        return status_msg == other.Cast<KeboolaPullBindData>().status_msg;
    }
};

struct KeboolaPullState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> KeboolaPullBindFn(ClientContext &context,
                                                   TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types,
                                                   vector<string> &names) {
    return_types = {LogicalType::VARCHAR};
    names        = {"status"};

    const std::string target = input.inputs[0].GetValue<std::string>();

    // Parse: db_name[."schema_name"[.table_name]]
    // Tokens split by '.' outside of double-quotes
    std::vector<std::string> parts;
    std::string cur;
    bool in_quote = false;
    for (char c : target) {
        if (c == '"') {
            in_quote = !in_quote;
        } else if (c == '.' && !in_quote) {
            parts.push_back(cur);
            cur.clear();
            continue;
        }
        cur += c;
    }
    if (!cur.empty()) parts.push_back(cur);

    if (parts.empty()) {
        throw BinderException("keboola_pull: empty target string");
    }

    auto strip_quotes = [](std::string s) -> std::string {
        if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
            return s.substr(1, s.size() - 2);
        }
        return s;
    };

    const std::string db_name = strip_quotes(parts[0]);
    auto &catalog = GetKeboolaCatalog(context, db_name);

    auto bind_data = make_uniq<KeboolaPullBindData>();

    if (parts.size() == 1) {
        catalog.PullAllTables(context);
        int total = 0;
        for (auto &skv : catalog.GetSchemas()) {
            total += (int)skv.second->GetTables().size();
        }
        bind_data->status_msg = "Pulled " + std::to_string(total) +
                                " table(s) in \"" + db_name + "\"";

    } else if (parts.size() == 2) {
        const std::string schema_name = strip_quotes(parts[1]);
        auto &schemas = catalog.GetSchemas();
        auto sit = schemas.find(schema_name);
        if (sit == schemas.end()) {
            throw CatalogException("Schema \"%s\" not found in catalog \"%s\"",
                                   schema_name, db_name);
        }
        sit->second->PullAllTables(context);
        int total = (int)sit->second->GetTables().size();
        bind_data->status_msg = "Pulled " + std::to_string(total) +
                                " table(s) from \"" + schema_name + "\"";

    } else {
        const std::string schema_name = strip_quotes(parts[1]);
        const std::string table_name  = strip_quotes(parts[2]);
        catalog.PullTable(context, schema_name, table_name);
        bind_data->status_msg = "Pulled \"" + schema_name + "." + table_name + "\"";
    }

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> KeboolaPullInitGlobal(
    ClientContext & /*context*/, TableFunctionInitInput & /*input*/) {
    return make_uniq<KeboolaPullState>();
}

static void KeboolaPullScan(ClientContext & /*context*/, TableFunctionInput &data_p,
                             DataChunk &output) {
    auto &bind  = data_p.bind_data->Cast<KeboolaPullBindData>();
    auto &state = data_p.global_state->Cast<KeboolaPullState>();

    if (state.done) {
        output.SetCardinality(0);
        return;
    }

    FlatVector::GetData<string_t>(output.data[0])[0] =
        StringVector::AddString(output.data[0], bind.status_msg);
    output.SetCardinality(1);
    state.done = true;
}

// ---------------------------------------------------------------------------
// RegisterKeboolaFunctions — called from LoadInternal in keboola_extension.cpp
// ---------------------------------------------------------------------------

void RegisterKeboolaFunctions(ExtensionLoader &loader) {
    // keboola_refresh_catalog(VARCHAR) → VARCHAR
    ScalarFunction refresh_func(
        "keboola_refresh_catalog",
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        KeboolaRefreshCatalogFun);
    loader.RegisterFunction(refresh_func);

    // keboola_tables(VARCHAR) → TABLE(schema_name, table_name, table_id, description, primary_key)
    TableFunction tables_func("keboola_tables",
                               {LogicalType::VARCHAR},
                               KeboolaTablesScan,
                               KeboolaTablesBindFn);
    tables_func.init_global = KeboolaTablesInitGlobal;
    loader.RegisterFunction(tables_func);

    // keboola_pull(VARCHAR) → TABLE(status VARCHAR)  — callable via CALL keboola_pull(...)
    TableFunction pull_func("keboola_pull",
                             {LogicalType::VARCHAR},
                             KeboolaPullScan,
                             KeboolaPullBindFn);
    pull_func.init_global = KeboolaPullInitGlobal;
    loader.RegisterFunction(pull_func);
}

} // namespace duckdb
