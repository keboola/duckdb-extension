#pragma once

// DuckDB main moved TableFilterSet to its own header (table_filter_set.hpp)
// and made the `filters` map private with a new iterator API.
// DuckDB v1.5.0 still has TableFilterSet in table_filter.hpp with a public map.
#if __has_include("duckdb/planner/table_filter_set.hpp")
#include "duckdb/planner/table_filter_set.hpp"   // DuckDB main
#define KEBOOLA_DUCKDB_NEW_FILTER_API 1
#else
#include "duckdb/planner/table_filter.hpp"        // DuckDB v1.5.0
#define KEBOOLA_DUCKDB_NEW_FILTER_API 0
#endif

#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

#include <string>
#include <vector>

namespace duckdb {

//! Generates SQL strings for the Keboola Query Service from DuckDB scan plan information.
class KeboolaSqlGenerator {
public:
    //! Build a SELECT statement from table info + optional pushed-down filters + projected columns.
    //!
    //! @param table_id        Keboola table ID, e.g. "in.c-crm.contacts"
    //! @param columns         Projected column names. If empty, generates SELECT *.
    //! @param filters         Pushed-down filters (may be null).
    //! @param limit           Row limit (-1 = no limit).
    //! @param all_column_names Full table column list indexed by table column position,
    //!                        used for WHERE clause column name resolution.
    //!                        If empty, falls back to using `columns` (only correct when
    //!                        all table columns are projected).
    static std::string BuildSelectSql(
        const std::string &table_id,
        const std::vector<std::string> &columns,
        const TableFilterSet *filters,
        int64_t limit = -1,
        const std::vector<std::string> &all_column_names = {});

    //! Quote an identifier with double-quotes, escaping internal double-quotes.
    static std::string EscapeIdentifier(const std::string &name);

    //! Wrap a string in single quotes, escaping internal single quotes as ''.
    static std::string EscapeStringLiteral(const std::string &value);

    //! Convert a DuckDB Value to its SQL literal representation.
    //! Numeric/boolean values are unquoted; strings are single-quoted.
    static std::string ValueToSqlLiteral(const Value &val);

private:
    //! Convert a single TableFilter on the given column to a SQL expression string.
    //! Returns empty string if the filter type is not supported (caller should skip it).
    static std::string FilterToSql(const std::string &col_name,
                                   const TableFilter &filter);
};

} // namespace duckdb
