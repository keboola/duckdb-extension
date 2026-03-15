#include "keboola_scan.hpp"

#include "http/query_service_client.hpp"
#include "util/sql_generator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaScanBindData
// ---------------------------------------------------------------------------

unique_ptr<FunctionData> KeboolaScanBindData::Copy() const {
    auto copy = make_uniq<KeboolaScanBindData>();
    copy->connection         = connection;
    copy->table_info         = table_info;
    copy->column_ids         = column_ids;
    copy->is_snapshot        = is_snapshot;
    copy->snapshot_rows      = snapshot_rows;
    copy->snapshot_null_mask = snapshot_null_mask;
    return std::move(copy);
}

bool KeboolaScanBindData::Equals(const FunctionData &other) const {
    const auto &o = other.Cast<KeboolaScanBindData>();
    return table_info.id == o.table_info.id && column_ids == o.column_ids;
}

// ---------------------------------------------------------------------------
// Type string → LogicalType helper
// ---------------------------------------------------------------------------

static LogicalType TypeStringToLogicalType(const std::string &type_str) {
    const std::string t = [&]() {
        std::string u = type_str;
        for (char &c : u) {
            c = static_cast<char>(::toupper(static_cast<unsigned char>(c)));
        }
        return u;
    }();

    if (t == "VARCHAR" || t == "TEXT" || t == "STRING" || t == "CHAR" ||
        t == "CHARACTER" || t == "NVARCHAR" || t == "NCHAR") {
        return LogicalType::VARCHAR;
    }
    if (t == "BIGINT" || t == "INT8" || t == "INT64") return LogicalType::BIGINT;
    if (t == "INTEGER" || t == "INT" || t == "INT4" || t == "INT32") return LogicalType::INTEGER;
    if (t == "SMALLINT" || t == "INT2" || t == "INT16") return LogicalType::SMALLINT;
    if (t == "TINYINT" || t == "INT1") return LogicalType::TINYINT;
    if (t == "DOUBLE" || t == "FLOAT8" || t == "DOUBLE PRECISION") return LogicalType::DOUBLE;
    if (t == "FLOAT" || t == "FLOAT4" || t == "REAL") return LogicalType::FLOAT;
    if (t == "BOOLEAN" || t == "BOOL") return LogicalType::BOOLEAN;
    if (t == "DATE") return LogicalType::DATE;
    if (t == "TIMESTAMP" || t == "DATETIME") return LogicalType::TIMESTAMP;
    if (t == "TIMESTAMPTZ" || t == "TIMESTAMP WITH TIME ZONE") return LogicalType::TIMESTAMP_TZ;
    if (t == "TIME") return LogicalType::TIME;

    return LogicalType::VARCHAR; // default
}

// ---------------------------------------------------------------------------
// String → DuckDB Value conversion
// ---------------------------------------------------------------------------

// NULL sentinel written by CsvBuilder for VARCHAR NULL values.
// U+E000 is a Unicode Private Use Area character (UTF-8: 0xEE 0x80 0x80).
static const char *kNullSentinel = "\xEE\x80\x80";

static Value StringToValue(const string &str, const LogicalType &type) {
    // Convert the NULL sentinel back to a proper NULL value regardless of type.
    if (str == kNullSentinel) {
        return Value(type);
    }
    switch (type.id()) {
        case LogicalTypeId::VARCHAR:
            return Value(str);

        case LogicalTypeId::TINYINT:
            try { return Value::TINYINT(static_cast<int8_t>(std::stoi(str))); }
            catch (...) { return Value(type); }

        case LogicalTypeId::SMALLINT:
            try { return Value::SMALLINT(static_cast<int16_t>(std::stoi(str))); }
            catch (...) { return Value(type); }

        case LogicalTypeId::INTEGER:
            try { return Value::INTEGER(std::stoi(str)); }
            catch (...) { return Value(type); }

        case LogicalTypeId::BIGINT:
            try { return Value::BIGINT(std::stoll(str)); }
            catch (...) { return Value(type); }

        case LogicalTypeId::UBIGINT:
            try { return Value::UBIGINT(std::stoull(str)); }
            catch (...) { return Value(type); }

        case LogicalTypeId::FLOAT:
            try { return Value::FLOAT(std::stof(str)); }
            catch (...) { return Value(type); }

        case LogicalTypeId::DOUBLE:
            try { return Value::DOUBLE(std::stod(str)); }
            catch (...) { return Value(type); }

        case LogicalTypeId::DECIMAL: {
            try { return Value::DOUBLE(std::stod(str)).DefaultCastAs(type); }
            catch (...) { return Value(type); }
        }

        case LogicalTypeId::BOOLEAN:
            if (str == "true" || str == "1" || str == "t" || str == "TRUE" || str == "T") {
                return Value::BOOLEAN(true);
            }
            return Value::BOOLEAN(false);

        case LogicalTypeId::DATE:
            try { return Value::DATE(Date::FromString(str)); }
            catch (...) {
                // GCP/BigQuery backend returns DATE as integer days-since-epoch
                try { return Value::DATE(date_t(std::stoi(str))); }
                catch (...) {}
                return Value(type);
            }

        case LogicalTypeId::TIMESTAMP:
            try { return Value::TIMESTAMP(Timestamp::FromString(str, false)); }
            catch (...) {
                // GCP Keboola backend returns TIMESTAMP as float seconds-since-epoch
                // (e.g. "1710433613.000000000"). Convert to DuckDB microseconds.
                try {
                    double secs = std::stod(str);
                    int64_t us = static_cast<int64_t>(secs * 1000000.0);
                    return Value::TIMESTAMP(timestamp_t(us));
                } catch (...) {}
                return Value(type);
            }

        case LogicalTypeId::TIMESTAMP_TZ:
            try { return Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromString(str, true))); }
            catch (...) { return Value(type); }

        case LogicalTypeId::TIME:
            try { return Value::TIME(Time::FromString(str)); }
            catch (...) { return Value(type); }

        default:
            return Value(str); // fallback to VARCHAR
    }
}

// ---------------------------------------------------------------------------
// Filter evaluation for snapshot rows
// ---------------------------------------------------------------------------

// Evaluate a single TableFilter against a cell value.
// Returns true if the cell passes the filter, false if it should be excluded.
static bool EvaluateTableFilter(const TableFilter &filter,
                                  bool is_null,
                                  const std::string &cell,
                                  const LogicalType &ltype) {
    switch (filter.filter_type) {
        case TableFilterType::IS_NULL:
            return is_null;
        case TableFilterType::IS_NOT_NULL:
            return !is_null;
        case TableFilterType::CONSTANT_COMPARISON: {
            if (is_null) {
                return false;  // NULL doesn't match any constant comparison
            }
            const auto &cf = filter.Cast<ConstantFilter>();
            Value row_val = StringToValue(cell, ltype);
            if (row_val.IsNull()) {
                return false;
            }
            // Cast row value to the filter constant's type for comparison
            Value casted;
            try {
                casted = row_val.DefaultCastAs(cf.constant.type());
            } catch (...) {
                return false;
            }
            return cf.Compare(casted);
        }
        default:
            return true;  // Unknown filter — pass through; DuckDB may re-check above
    }
}

// Apply all pushed-down filters from `filters` to the snapshot rows, returning
// a vector of indices of rows that pass all filters.
static std::vector<idx_t> FilterSnapshotRows(
    const TableFilterSet &filters,
    const std::vector<std::vector<std::string>> &rows,
    const std::vector<std::vector<bool>> &null_mask,
    const std::vector<int> &data_col_map,
    const std::vector<LogicalType> &col_types) {

    std::vector<idx_t> result;
    result.reserve(rows.size());

    for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
        const auto &row = rows[row_idx];
        const auto *row_nulls = (row_idx < null_mask.size()) ? &null_mask[row_idx] : nullptr;

        bool passes = true;
#if KEBOOLA_DUCKDB_NEW_FILTER_API
        for (const auto &entry : filters) {
            idx_t filter_col = entry.ColumnIndex();
            const TableFilter &tf = entry.Filter();
#else
        for (const auto &flt_kv : filters.filters) {
            idx_t filter_col = flt_kv.first;
            const TableFilter &tf = *flt_kv.second;
#endif

            int dc = (filter_col < data_col_map.size()) ? data_col_map[filter_col]
                                                        : static_cast<int>(filter_col);
            if (dc < 0) {
                // row-id virtual column — skip filter (row-ids always non-null)
                continue;
            }

            bool is_null = false;
            if (row_nulls && static_cast<idx_t>(dc) < row_nulls->size()) {
                is_null = (*row_nulls)[static_cast<idx_t>(dc)];
            }
            if (static_cast<idx_t>(dc) >= row.size()) {
                is_null = true;
            }

            const std::string &cell = is_null ? "" : row[static_cast<idx_t>(dc)];
            const LogicalType &ltype = (filter_col < col_types.size())
                                           ? col_types[filter_col]
                                           : LogicalType::VARCHAR;

            if (!EvaluateTableFilter(tf, is_null, cell, ltype)) {
                passes = false;
                break;
            }
        }
        if (passes) {
            result.push_back(row_idx);
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// Bind function
//
// NOTE: For the TableCatalogEntry path, DuckDB calls GetScanFunction which
// pre-populates bind_data and passes it directly to LogicalGet — the bind
// callback below is NOT called for that path.
//
// However we still register it so the TableFunction is properly formed
// (e.g. when used directly as a table function rather than via catalog).
// ---------------------------------------------------------------------------

static unique_ptr<FunctionData> KeboolaScanBind(ClientContext & /*context*/,
                                                  TableFunctionBindInput & /*input*/,
                                                  vector<LogicalType> & /*return_types*/,
                                                  vector<string> & /*names*/) {
    // For Keboola catalog tables, GetScanFunction pre-populates bind_data and
    // DuckDB passes it directly to LogicalGet — this bind callback is never called
    // in the standard catalog table access path.
    throw InternalException(
        "KeboolaScanBind should not be called directly; use ATTACH TYPE keboola.");
}

// ---------------------------------------------------------------------------
// InitGlobal function
// ---------------------------------------------------------------------------

static unique_ptr<GlobalTableFunctionState> KeboolaScanInitGlobal(ClientContext & /*context*/,
                                                                    TableFunctionInitInput &input) {
    // bind_data is const here (input.bind_data is optional_ptr<const FunctionData>)
    const auto &bind = input.bind_data->Cast<KeboolaScanBindData>();
    const auto &conn = *bind.connection;

    auto gstate = make_uniq<KeboolaScanGlobalState>();

    const auto &all_cols = bind.table_info.columns;

    // Resolve projected columns from column_ids (set during optimizer pushdown).
    // input.column_ids is populated by DuckDB's projection pushdown optimizer.
    std::vector<std::string> projected_names;

    const auto &col_ids = input.column_ids;

    // Build output column map: maps output col index → data col index (or -1 = row-id).
    // This lets the scan function output the correct row index for virtual row-id columns
    // rather than trying to cast actual row data to BIGINT.
    if (!col_ids.empty()) {
        int data_idx = 0;
        for (auto cid : col_ids) {
            if (cid == COLUMN_IDENTIFIER_ROW_ID) {
                gstate->column_types.push_back(LogicalType::BIGINT);
                gstate->data_col_map.push_back(-1);
                continue;
            }
            if (cid < all_cols.size()) {
                projected_names.push_back(all_cols[cid].name);
                gstate->column_types.push_back(TypeStringToLogicalType(all_cols[cid].duckdb_type));
            } else {
                gstate->column_types.push_back(LogicalType::VARCHAR);
            }
            gstate->data_col_map.push_back(data_idx++);
        }
    }

    // If no real columns projected, we still need to fetch row data so that COUNT(*) and
    // row-id-only queries can determine the number of rows.  Populate projected_names
    // with all columns but leave column_types / data_col_map untouched — those already
    // correctly describe the OUTPUT layout (e.g. a single BIGINT row-id column).
    if (projected_names.empty()) {
        for (const auto &col : all_cols) {
            projected_names.push_back(col.name);
        }
        // col_ids was completely empty (no pushdown at all): also set output mapping.
        if (gstate->column_types.empty()) {
            int data_idx = 0;
            for (const auto &col : all_cols) {
                gstate->column_types.push_back(TypeStringToLogicalType(col.duckdb_type));
                gstate->data_col_map.push_back(data_idx++);
            }
        }
    }

    if (bind.is_snapshot && bind.snapshot_rows != nullptr) {
        // Snapshot mode: use pre-fetched rows — no Query Service call.
        // Apply pushed-down filters so that WHERE clauses work correctly
        // (DuckDB does not add a PhysicalFilter on top when filter_pushdown = true).
        const auto &src_rows    = *bind.snapshot_rows;
        const auto &src_mask    = bind.snapshot_null_mask ? *bind.snapshot_null_mask
                                                           : std::vector<std::vector<bool>>{};
        const TableFilterSet *filters = input.filters.get();

#if KEBOOLA_DUCKDB_NEW_FILTER_API
        if (filters && filters->HasFilters()) {
#else
        if (filters && !filters->filters.empty()) {
#endif
            // Evaluate filters and collect passing row indices.
            auto passing = FilterSnapshotRows(*filters, src_rows, src_mask,
                                              gstate->data_col_map, gstate->column_types);
            gstate->rows.resize(passing.size());
            gstate->null_mask.resize(passing.size());
            for (idx_t i = 0; i < passing.size(); i++) {
                idx_t src = passing[i];
                gstate->rows[i].assign(src_rows[src].begin(), src_rows[src].end());
                if (src < src_mask.size()) {
                    gstate->null_mask[i].assign(src_mask[src].begin(), src_mask[src].end());
                }
            }
        } else {
            // No filters — copy all rows.
            gstate->rows.resize(src_rows.size());
            for (idx_t i = 0; i < src_rows.size(); i++) {
                gstate->rows[i].assign(src_rows[i].begin(), src_rows[i].end());
            }
            gstate->null_mask.resize(src_mask.size());
            for (idx_t i = 0; i < src_mask.size(); i++) {
                gstate->null_mask[i].assign(src_mask[i].begin(), src_mask[i].end());
            }
        }
    } else {
        // Live mode: query via the Query Service
        // Get pushed-down filters
        const TableFilterSet *filters = input.filters.get();

        // Build full column name list for correct WHERE clause column name resolution.
        // Filter column indices are table-level positions; projected_names may be a subset.
        std::vector<std::string> all_column_names;
        all_column_names.reserve(all_cols.size());
        for (const auto &col : all_cols) {
            all_column_names.push_back(col.name);
        }

        // Build SQL
        std::string sql = KeboolaSqlGenerator::BuildSelectSql(
            bind.table_info.id,
            projected_names,
            filters,
            -1,
            all_column_names
        );

        QueryServiceClient qsc(
            conn.service_urls.query_url,
            conn.token,
            conn.branch_id,
            conn.workspace_id
        );

        try {
            auto result = qsc.ExecuteQuery(sql);
            gstate->rows.resize(result.rows.size());
            for (idx_t i = 0; i < result.rows.size(); i++) {
                gstate->rows[i].assign(result.rows[i].begin(), result.rows[i].end());
            }
            gstate->null_mask.resize(result.null_mask.size());
            for (idx_t i = 0; i < result.null_mask.size(); i++) {
                gstate->null_mask[i].assign(result.null_mask[i].begin(), result.null_mask[i].end());
            }
        } catch (const std::exception &e) {
            throw IOException("Keboola scan failed for table '%s': %s",
                              bind.table_info.id, std::string(e.what()));
        }
    }

    gstate->position = 0;
    gstate->done = gstate->rows.empty();

    return std::move(gstate);
}

// ---------------------------------------------------------------------------
// Scan function
// ---------------------------------------------------------------------------

static void KeboolaScanFunction(ClientContext & /*context*/,
                                 TableFunctionInput &data_p,
                                 DataChunk &output) {
    auto &gstate = data_p.global_state->Cast<KeboolaScanGlobalState>();

    if (gstate.done) {
        output.SetCardinality(0);
        return;
    }

    // Column types were resolved during InitGlobal and stored on the global state.
    const auto &col_types = gstate.column_types;

    idx_t count = 0;
    idx_t col_count = output.ColumnCount();

    while (count < STANDARD_VECTOR_SIZE) {
        idx_t row_idx = gstate.position.fetch_add(1);
        if (row_idx >= gstate.rows.size()) {
            gstate.done = true;
            break;
        }

        const auto &row = gstate.rows[row_idx];
        const auto *row_nulls = (row_idx < gstate.null_mask.size())
                                     ? &gstate.null_mask[row_idx]
                                     : nullptr;

        for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
            auto &vec = output.data[col_idx];

            // Resolve which data column this output column maps to (-1 = row-id).
            int dc = (col_idx < gstate.data_col_map.size())
                         ? gstate.data_col_map[col_idx]
                         : static_cast<int>(col_idx);

            if (dc == -1) {
                // Virtual row-id column: output the row index as BIGINT.
                vec.SetValue(count, Value::BIGINT(static_cast<int64_t>(row_idx)));
                continue;
            }

            bool is_null = false;
            if (row_nulls && static_cast<idx_t>(dc) < row_nulls->size()) {
                is_null = (*row_nulls)[static_cast<idx_t>(dc)];
            }
            if (static_cast<idx_t>(dc) >= row.size()) {
                is_null = true;
            }

            if (is_null) {
                FlatVector::SetNull(vec, count, true);
            } else {
                const std::string &cell = row[static_cast<idx_t>(dc)];
                LogicalType ltype = (col_idx < col_types.size())
                                        ? col_types[col_idx]
                                        : LogicalType::VARCHAR;
                Value val = StringToValue(cell, ltype);
                vec.SetValue(count, val);
            }
        }

        count++;
    }

    output.SetCardinality(count);
}

// ---------------------------------------------------------------------------
// KeboolaGetScanFunction
// ---------------------------------------------------------------------------

static BindInfo KeboolaScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
    if (bind_data) {
        const auto &d = bind_data->Cast<KeboolaScanBindData>();
        if (d.table_entry) {
            // BindInfo(TableCatalogEntry &) requires non-const; the entry is owned by
            // the catalog and lives as long as the connection, so the cast is safe.
            return BindInfo(const_cast<TableCatalogEntry &>(*d.table_entry));
        }
    }
    return BindInfo(ScanType::TABLE);
}

TableFunction KeboolaGetScanFunction() {
    TableFunction func("keboola_scan", {}, KeboolaScanFunction, KeboolaScanBind);
    func.init_global         = KeboolaScanInitGlobal;
    func.filter_pushdown     = true;
    func.projection_pushdown = true;
    func.get_bind_info       = KeboolaScanGetBindInfo;
    return func;
}

} // namespace duckdb
