#pragma once

// Cross-version compatibility helpers for DuckDB API drift between the v1.5.x
// releases and current main. Follows the SFINAE-dispatch style already used in
// keboola_catalog.cpp: no version macros, the available overload wins.
//
// What changed on main (2026-06/07):
//   * identifiers (table/column/constraint names) moved from std::string to
//     duckdb::Identifier — case-insensitive, explicit conversion to string;
//   * CreateTableInfo lost its public catalog/schema/table string members in
//     favour of SetCatalog()/SetSchema()/SetTableName() + GetTableName().

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/column_definition.hpp"

#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace duckdb {
namespace keboola_compat {

// ---------------------------------------------------------------------------
// NameToString — identifier-ish value to std::string
// ---------------------------------------------------------------------------

//! v1.5.x: names are plain strings — pass through.
inline const std::string &NameToString(const std::string &name) {
    return name;
}

//! main: names are duckdb::Identifier — unwrap the raw value.
template <class T>
inline auto NameToString(const T &name) -> decltype(name.GetIdentifierName()) {
    return name.GetIdentifierName();
}

//! The C++ type a ColumnDefinition name has in this DuckDB version
//! (std::string on v1.5.x, Identifier on main).
using column_name_t =
    typename std::decay<decltype(std::declval<const ColumnDefinition &>().GetName())>::type;

//! Build a ColumnDefinition from a plain string name on either version — the
//! Identifier constructor from string is explicit on main, so the name must
//! be wrapped in the version's native name type.
inline ColumnDefinition MakeColumnDefinition(const std::string &name, LogicalType type) {
    return ColumnDefinition(column_name_t(name), std::move(type));
}

// ---------------------------------------------------------------------------
// CreateTableInfo qualified-name access
// ---------------------------------------------------------------------------

//! main: setter-based API keyed off the GetTableName() accessor.
template <class Info>
inline auto SetQualifiedName(Info &info, const std::string &catalog, const std::string &schema,
                             const std::string &table, int)
    -> decltype(info.GetTableName(), void()) {
    using name_t = typename std::decay<decltype(info.GetTableName())>::type;
    info.SetCatalog(name_t(catalog));
    info.SetSchema(name_t(schema));
    info.SetTableName(name_t(table));
}

//! v1.5.x: public string members.
template <class Info>
inline void SetQualifiedName(Info &info, const std::string &catalog, const std::string &schema,
                             const std::string &table, long) {
    info.catalog = catalog;
    info.schema = schema;
    info.table = table;
}

inline void SetQualifiedName(CreateTableInfo &info, const std::string &catalog,
                             const std::string &schema, const std::string &table) {
    SetQualifiedName(info, catalog, schema, table, 0);
}

//! main: read the table name through the accessor.
template <class Info>
inline auto GetTableString(const Info &info, int) -> decltype(NameToString(info.GetTableName())) {
    return NameToString(info.GetTableName());
}

//! v1.5.x: read the public string member.
template <class Info>
inline const std::string &GetTableString(const Info &info, long) {
    return info.table;
}

inline const std::string &GetTableString(const CreateTableInfo &info) {
    return GetTableString(info, 0);
}

// ---------------------------------------------------------------------------
// Name vectors (e.g. UniqueConstraint::GetColumnNames)
// ---------------------------------------------------------------------------

//! Convert a vector of string-or-Identifier names to plain strings.
template <class NameVec>
inline std::vector<std::string> NamesToStrings(const NameVec &names) {
    std::vector<std::string> out;
    out.reserve(names.size());
    for (auto &n : names) {
        out.push_back(NameToString(n));
    }
    return out;
}

} // namespace keboola_compat
} // namespace duckdb
