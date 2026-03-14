#include "util/sql_generator.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
// TableFilterSet include is handled in the header (table_filter_set.hpp vs table_filter.hpp)

#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// EscapeIdentifier
// ---------------------------------------------------------------------------

std::string KeboolaSqlGenerator::EscapeIdentifier(const std::string &name) {
    std::string result = "\"";
    for (char c : name) {
        if (c == '"') {
            result += "\"\""; // double up double-quotes
        } else {
            result += c;
        }
    }
    result += '"';
    return result;
}

// ---------------------------------------------------------------------------
// EscapeStringLiteral
// ---------------------------------------------------------------------------

std::string KeboolaSqlGenerator::EscapeStringLiteral(const std::string &value) {
    std::string result = "'";
    for (char c : value) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    result += '\'';
    return result;
}

// ---------------------------------------------------------------------------
// ValueToSqlLiteral
// ---------------------------------------------------------------------------

std::string KeboolaSqlGenerator::ValueToSqlLiteral(const Value &val) {
    if (val.IsNull()) {
        return "NULL";
    }

    switch (val.type().id()) {
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "TRUE" : "FALSE";
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
        case LogicalTypeId::UTINYINT:
        case LogicalTypeId::USMALLINT:
        case LogicalTypeId::UINTEGER:
        case LogicalTypeId::UBIGINT:
        case LogicalTypeId::HUGEINT:
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
        case LogicalTypeId::DECIMAL:
            return val.ToString();
        default:
            // VARCHAR, DATE, TIMESTAMP, etc. — quote as strings
            return EscapeStringLiteral(val.ToString());
    }
}

// ---------------------------------------------------------------------------
// FilterToSql
// ---------------------------------------------------------------------------

std::string KeboolaSqlGenerator::FilterToSql(const std::string &col_name,
                                               const TableFilter &filter) {
    std::string col = EscapeIdentifier(col_name);

    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            const auto &cf = filter.Cast<ConstantFilter>();
            std::string op;
            switch (cf.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:            op = "=";  break;
                case ExpressionType::COMPARE_NOTEQUAL:         op = "<>"; break;
                case ExpressionType::COMPARE_LESSTHAN:         op = "<";  break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "<="; break;
                case ExpressionType::COMPARE_GREATERTHAN:      op = ">";  break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
                default:
                    return ""; // unsupported comparison — skip
            }
            return col + " " + op + " " + ValueToSqlLiteral(cf.constant);
        }

        case TableFilterType::IS_NULL:
            return col + " IS NULL";

        case TableFilterType::IS_NOT_NULL:
            return col + " IS NOT NULL";

        case TableFilterType::CONJUNCTION_AND: {
            const auto &cf = filter.Cast<ConjunctionAndFilter>();
            std::ostringstream oss;
            bool first = true;
            for (const auto &child : cf.child_filters) {
                std::string child_sql = FilterToSql(col_name, *child);
                if (child_sql.empty()) continue;
                if (!first) oss << " AND ";
                oss << "(" << child_sql << ")";
                first = false;
            }
            std::string result = oss.str();
            return result.empty() ? "" : result;
        }

        case TableFilterType::CONJUNCTION_OR: {
            const auto &cf = filter.Cast<ConjunctionOrFilter>();
            std::ostringstream oss;
            bool first = true;
            for (const auto &child : cf.child_filters) {
                std::string child_sql = FilterToSql(col_name, *child);
                if (child_sql.empty()) continue;
                if (!first) oss << " OR ";
                oss << "(" << child_sql << ")";
                first = false;
            }
            std::string result = oss.str();
            return result.empty() ? "" : result;
        }

        case TableFilterType::IN_FILTER: {
            const auto &inf = filter.Cast<InFilter>();
            if (inf.values.empty()) {
                return "FALSE"; // IN () is always false
            }
            std::ostringstream oss;
            oss << col << " IN (";
            for (size_t i = 0; i < inf.values.size(); i++) {
                if (i > 0) oss << ", ";
                oss << ValueToSqlLiteral(inf.values[i]);
            }
            oss << ")";
            return oss.str();
        }

        default:
            return ""; // unknown filter — skip, DuckDB will apply locally
    }
}

// ---------------------------------------------------------------------------
// BuildSelectSql
// ---------------------------------------------------------------------------

std::string KeboolaSqlGenerator::BuildSelectSql(
        const std::string &table_id,
        const std::vector<std::string> &columns,
        const TableFilterSet *filters,
        int64_t limit) {

    std::ostringstream sql;

    // SELECT clause
    sql << "SELECT ";
    if (columns.empty()) {
        sql << "*";
    } else {
        for (size_t i = 0; i < columns.size(); i++) {
            if (i > 0) sql << ", ";
            sql << EscapeIdentifier(columns[i]);
        }
    }

    // FROM clause — split table_id on the LAST dot
    // e.g. "in.c-crm.contacts" -> schema="in.c-crm", table="contacts"
    auto last_dot = table_id.rfind('.');
    std::string schema_part;
    std::string table_part;
    if (last_dot != std::string::npos) {
        schema_part = table_id.substr(0, last_dot);
        table_part  = table_id.substr(last_dot + 1);
    } else {
        schema_part = "";
        table_part  = table_id;
    }

    sql << " FROM ";
    if (!schema_part.empty()) {
        sql << EscapeIdentifier(schema_part) << ".";
    }
    sql << EscapeIdentifier(table_part);

    // WHERE clause from pushed-down filters
    // DuckDB main (KEBOOLA_DUCKDB_NEW_FILTER_API=1): filters map is private;
    //   iterate via begin()/end() — entry.ColumnIndex() returns idx_t, entry.Filter() returns TableFilter&
    // DuckDB v1.5.0 (KEBOOLA_DUCKDB_NEW_FILTER_API=0): filters map is public as map<idx_t, unique_ptr<TableFilter>>
#if KEBOOLA_DUCKDB_NEW_FILTER_API
    if (filters && filters->HasFilters()) {
        std::vector<std::string> conditions;
        for (const auto &entry : *filters) {
            idx_t col_idx = entry.ColumnIndex();
            const TableFilter &tf = entry.Filter();
            if (columns.empty() || col_idx >= columns.size()) continue;
            std::string cond = FilterToSql(columns[col_idx], tf);
            if (!cond.empty()) conditions.push_back(cond);
        }
        if (!conditions.empty()) {
            sql << " WHERE ";
            for (size_t i = 0; i < conditions.size(); i++) {
                if (i > 0) sql << " AND ";
                sql << "(" << conditions[i] << ")";
            }
        }
    }
#else
    if (filters && !filters->filters.empty()) {
        std::vector<std::string> conditions;
        for (const auto &kv : filters->filters) {
            idx_t col_idx = kv.first;
            const TableFilter &tf = *kv.second;
            if (columns.empty() || col_idx >= columns.size()) continue;
            std::string cond = FilterToSql(columns[col_idx], tf);
            if (!cond.empty()) conditions.push_back(cond);
        }
        if (!conditions.empty()) {
            sql << " WHERE ";
            for (size_t i = 0; i < conditions.size(); i++) {
                if (i > 0) sql << " AND ";
                sql << "(" << conditions[i] << ")";
            }
        }
    }
#endif

    // LIMIT clause
    if (limit > 0) {
        sql << " LIMIT " << limit;
    }

    return sql.str();
}

} // namespace duckdb
