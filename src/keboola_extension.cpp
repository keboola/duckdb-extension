#define DUCKDB_EXTENSION_MAIN

#include "keboola_extension.hpp"
#include "keboola_storage.hpp"
#include "keboola_secret.hpp"
#include "keboola_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// keboola_version() SQL scalar function
// Returns the extension version string as VARCHAR.
// ---------------------------------------------------------------------------
static void KeboolaVersionScalarFun(DataChunk & /*args*/, ExpressionState & /*state*/,
                                     Vector &result) {
    auto &result_vector = result;
    result_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::GetData<string_t>(result_vector)[0] =
        StringVector::AddString(result_vector, "0.1.0");
}

// ---------------------------------------------------------------------------
// LoadInternal — called from both static and dynamic load paths
// ---------------------------------------------------------------------------
static void LoadInternal(DatabaseInstance &db) {
    // Register keboola_version() scalar function
    ScalarFunction version_func("keboola_version",
                                 {},                     // no arguments
                                 LogicalType::VARCHAR,
                                 KeboolaVersionScalarFun);
    ExtensionUtil::RegisterFunction(db, version_func);

    // Register the "keboola" secret type (TOKEN, URL, BRANCH)
    RegisterKeboolaSecret(db);

    // Register the keboola storage extension (handles ATTACH TYPE keboola)
    auto &config = DBConfig::GetConfig(db);
    config.storage_extensions["keboola"] = make_uniq<KeboolaStorageExtension>();

    // Register Phase 6 utility functions
    RegisterKeboolaFunctions(db);
}

// ---------------------------------------------------------------------------
// KeboolaExtension methods
// ---------------------------------------------------------------------------
void KeboolaExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string KeboolaExtension::Name() {
    return "keboola";
}

std::string KeboolaExtension::Version() const {
#ifdef EXT_VERSION_KEBOOLA
    return EXT_VERSION_KEBOOLA;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

// ---------------------------------------------------------------------------
// Extern "C" entry points — DuckDB dynamic loading API
// ---------------------------------------------------------------------------
extern "C" {

DUCKDB_EXTENSION_API void keboola_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::KeboolaExtension>();
}

DUCKDB_EXTENSION_API const char *keboola_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
