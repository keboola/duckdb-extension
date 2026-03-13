#pragma once

#include <string>

namespace duckdb {

//! Maps Keboola/Snowflake column type metadata to a DuckDB type string.
//!
//! Priority:
//!  1. KBC.datatype.type  (Snowflake native type)
//!  2. KBC.datatype.basetype (Keboola base type)
//!  3. Default: VARCHAR
//!
//! precision and scale are used for NUMBER/DECIMAL columns.
std::string MapKeboolaTypeToDuckDB(const std::string &kbc_type,
                                   const std::string &basetype,
                                   int precision,
                                   int scale);

} // namespace duckdb
