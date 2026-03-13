#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Register the "keboola" secret type with DuckDB's SecretManager.
//! After calling this, users can do:
//!   CREATE SECRET my_kbc (
//!       TYPE keboola,
//!       TOKEN 'sapi-token-xxx',
//!       URL  'https://connection.keboola.com',
//!       BRANCH 'main'
//!   );
void RegisterKeboolaSecret(DatabaseInstance &db);

} // namespace duckdb
