#define DUCKDB_EXTENSION_MAIN

#include "keboola_extension.hpp"
#include "keboola_storage.hpp"
#include "keboola_secret.hpp"
#include "keboola_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/config.hpp"

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
static void LoadInternal(ExtensionLoader &loader) {
    // Register keboola_version() scalar function
    ScalarFunction version_func("keboola_version",
                                 {},                     // no arguments
                                 LogicalType::VARCHAR,
                                 KeboolaVersionScalarFun);
    loader.RegisterFunction(version_func);

    // Register the "keboola" secret type (TOKEN, URL, BRANCH)
    RegisterKeboolaSecret(loader);

    // Register the keboola storage extension (handles ATTACH TYPE keboola)
    auto &db = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(db);
    StorageExtension::Register(config, "keboola", make_shared_ptr<KeboolaStorageExtension>());

    // Register Phase 6 utility functions
    RegisterKeboolaFunctions(loader);
}

// ---------------------------------------------------------------------------
// KeboolaExtension methods
// ---------------------------------------------------------------------------
void KeboolaExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(keboola, loader) {
    duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *keboola_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
