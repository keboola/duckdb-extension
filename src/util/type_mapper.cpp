#include "util/type_mapper.hpp"

#include <algorithm>
#include <string>

namespace duckdb {

static std::string ToUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(::toupper(c));
    });
    return s;
}

//! Map from Snowflake / KBC native type string to a DuckDB type string.
static std::string MapFromSnowflakeType(const std::string &raw_type, int precision, int scale) {
    const std::string t = ToUpper(raw_type);

    if (t == "VARCHAR" || t == "TEXT" || t == "STRING" || t == "CHAR" ||
        t == "CHARACTER" || t == "NCHAR" || t == "NVARCHAR" || t == "NVARCHAR2" ||
        t == "CHAR VARYING" || t == "NCHAR VARYING") {
        return "VARCHAR";
    }
    if (t == "NUMBER" || t == "NUMERIC" || t == "DECIMAL") {
        if (scale == 0) {
            return "BIGINT";
        }
        if (precision > 0) {
            return "DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
        }
        return "DOUBLE";
    }
    if (t == "INT" || t == "INTEGER" || t == "BIGINT" || t == "SMALLINT" ||
        t == "TINYINT" || t == "BYTEINT") {
        return "BIGINT";
    }
    if (t == "FLOAT" || t == "FLOAT4" || t == "FLOAT8" || t == "REAL" ||
        t == "DOUBLE" || t == "DOUBLE PRECISION") {
        return "DOUBLE";
    }
    if (t == "BOOLEAN") {
        return "BOOLEAN";
    }
    if (t == "DATE") {
        return "DATE";
    }
    if (t == "TIMESTAMP_NTZ" || t == "DATETIME" || t == "TIMESTAMP" ||
        t == "TIMESTAMP WITHOUT TIME ZONE") {
        return "TIMESTAMP";
    }
    if (t == "TIMESTAMP_TZ" || t == "TIMESTAMP_LTZ" ||
        t == "TIMESTAMP WITH TIME ZONE" || t == "TIMESTAMP WITH LOCAL TIME ZONE") {
        return "TIMESTAMPTZ";
    }
    if (t == "TIME" || t == "TIME WITHOUT TIME ZONE") {
        return "TIME";
    }
    if (t == "ARRAY" || t == "OBJECT" || t == "VARIANT") {
        // Represent semi-structured types as JSON string for now
        return "VARCHAR";
    }
    if (t == "BINARY" || t == "VARBINARY") {
        return "BLOB";
    }

    // Unknown Snowflake type — return empty to trigger basetype fallback
    return "";
}

//! Map from Keboola basetype to DuckDB type.
static std::string MapFromBasetype(const std::string &basetype) {
    const std::string t = ToUpper(basetype);

    if (t == "STRING") return "VARCHAR";
    if (t == "INTEGER") return "BIGINT";
    if (t == "NUMERIC") return "DOUBLE";
    if (t == "FLOAT")   return "DOUBLE";
    if (t == "BOOLEAN") return "BOOLEAN";
    if (t == "DATE")    return "DATE";
    if (t == "TIMESTAMP") return "TIMESTAMP";

    return "";
}

std::string MapKeboolaTypeToDuckDB(const std::string &kbc_type,
                                   const std::string &basetype,
                                   int precision,
                                   int scale) {
    // Priority 1: Snowflake / KBC native type
    if (!kbc_type.empty()) {
        auto result = MapFromSnowflakeType(kbc_type, precision, scale);
        if (!result.empty()) {
            return result;
        }
    }

    // Priority 2: Keboola base type
    if (!basetype.empty()) {
        auto result = MapFromBasetype(basetype);
        if (!result.empty()) {
            return result;
        }
    }

    // Default
    return "VARCHAR";
}

} // namespace duckdb
