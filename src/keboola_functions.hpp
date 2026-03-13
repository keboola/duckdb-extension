#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Register all Phase 6 utility functions:
//!   keboola_refresh_catalog(db_name VARCHAR) → VARCHAR
//!   keboola_tables(db_name VARCHAR) → TABLE(...)
//!   keboola_pull(target VARCHAR) → VARCHAR
void RegisterKeboolaFunctions(ExtensionLoader &loader);

} // namespace duckdb
