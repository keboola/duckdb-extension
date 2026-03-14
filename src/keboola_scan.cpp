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

static Value StringToValue(const string &str, const LogicalType &type) {
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
            catch (...) { return Value(type); }

        case LogicalTypeId::TIMESTAMP:
            try { return Value::TIMESTAMP(Timestamp::FromString(str, false)); }
            catch (...) { return Value(type); }

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
        // Snapshot mode: use pre-fetched rows — no Query Service call
        const auto &src_rows = *bind.snapshot_rows;
        gstate->rows.resize(src_rows.size());
        for (idx_t i = 0; i < src_rows.size(); i++) {
            gstate->rows[i].assign(src_rows[i].begin(), src_rows[i].end());
        }
        if (bind.snapshot_null_mask != nullptr) {
            const auto &src_mask = *bind.snapshot_null_mask;
            gstate->null_mask.resize(src_mask.size());
            for (idx_t i = 0; i < src_mask.size(); i++) {
                gstate->null_mask[i].assign(src_mask[i].begin(), src_mask[i].end());
            }
        }
    } else {
        // Live mode: query via the Query Service
        // Get pushed-down filters
        const TableFilterSet *filters = input.filters.get();

        // Build SQL
        std::string sql = KeboolaSqlGenerator::BuildSelectSql(
            bind.table_info.id,
            projected_names,
            filters,
            -1
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
