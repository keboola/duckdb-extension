#include "util/csv_builder.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types.hpp"

#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

// NULL sentinel: Unicode Private Use Area character U+E000 (UTF-8: 0xEE 0x80 0x80).
// Used only for VARCHAR NULL values so that NULL and empty string are distinguishable
// even in Keboola's untyped (string) tables where the CSV importer cannot convert
// empty fields to SQL NULL.  Non-VARCHAR NULLs still use an unquoted empty field,
// which is correctly stored as NULL by Keboola's Snowflake backend for typed columns.
static const std::string kNullSentinel = "\xEE\x80\x80";

// ---------------------------------------------------------------------------
// QuoteField — RFC 4180
// ---------------------------------------------------------------------------

std::string CsvBuilder::QuoteField(const std::string &field) {
    // Check whether quoting is needed:
    // - empty string must be quoted ("") so it remains a distinct empty value and
    //   cannot be confused with an unquoted NULL-sentinel field.
    // - fields containing CSV special characters always need quoting.
    bool needs_quoting = field.empty();
    if (!needs_quoting) {
        for (char c : field) {
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needs_quoting = true;
                break;
            }
        }
    }

    if (!needs_quoting) {
        return field;
    }

    // Wrap in double quotes and escape inner double-quotes as ""
    std::string result;
    result.reserve(field.size() + 2);
    result += '"';
    for (char c : field) {
        if (c == '"') {
            result += "\"\"";
        } else {
            result += c;
        }
    }
    result += '"';
    return result;
}

// ---------------------------------------------------------------------------
// ValueToString — convert DuckDB Value to CSV-appropriate string
// ---------------------------------------------------------------------------

std::string CsvBuilder::ValueToString(const Value &val) {
    if (val.IsNull()) {
        // NULL → empty field (no quotes); callers that need VARCHAR NULL
        // should use the kNullSentinel path in AddChunk instead.
        return "";
    }

    const auto type_id = val.type().id();

    switch (type_id) {
        case LogicalTypeId::BOOLEAN:
            return val.GetValue<bool>() ? "true" : "false";

        case LogicalTypeId::DATE: {
            // Format: YYYY-MM-DD
            date_t d = val.GetValue<date_t>();
            return Date::ToString(d);
        }

        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::TIMESTAMP_SEC:
        case LogicalTypeId::TIMESTAMP_MS:
        case LogicalTypeId::TIMESTAMP_NS: {
            // Format: YYYY-MM-DD HH:MM:SS+00:00
            // Append UTC offset so Snowflake TIMESTAMP_LTZ columns store in UTC.
            timestamp_t ts = val.GetValue<timestamp_t>();
            return Timestamp::ToString(ts) + " +00:00";
        }

        case LogicalTypeId::TIMESTAMP_TZ: {
            timestamp_t ts = val.GetValue<timestamp_t>();
            return Timestamp::ToString(ts) + " +00:00";
        }

        default:
            // For all other types, use DuckDB's built-in ToString which handles
            // integers, floats, decimals, varchar, etc.
            return val.ToString();
    }
}

// ---------------------------------------------------------------------------
// AddHeader
// ---------------------------------------------------------------------------

void CsvBuilder::AddHeader(const std::vector<std::string> &column_names) {
    bool first = true;
    for (const auto &col : column_names) {
        if (!first) {
            buffer_ << ',';
        }
        buffer_ << QuoteField(col);
        first = false;
    }
    buffer_ << "\r\n";
}

// ---------------------------------------------------------------------------
// AddRow
// ---------------------------------------------------------------------------

void CsvBuilder::AddRow(const std::vector<std::string> &values) {
    bool first = true;
    for (const auto &val : values) {
        if (!first) {
            buffer_ << ',';
        }
        buffer_ << QuoteField(val);
        first = false;
    }
    buffer_ << "\r\n";
    row_count_++;
}

// ---------------------------------------------------------------------------
// AddChunk
// ---------------------------------------------------------------------------

void CsvBuilder::AddChunk(const DataChunk &chunk, const std::vector<std::string> & /*column_names*/) {
    const idx_t num_rows = chunk.size();
    const idx_t num_cols = chunk.ColumnCount();

    for (idx_t row_idx = 0; row_idx < num_rows; row_idx++) {
        for (idx_t col_idx = 0; col_idx < num_cols; col_idx++) {
            if (col_idx > 0) {
                buffer_ << ',';
            }

            Value val = chunk.data[col_idx].GetValue(row_idx);
            if (val.IsNull()) {
                if (val.type().id() == LogicalTypeId::VARCHAR) {
                    // VARCHAR NULL: write the private-use sentinel so NULL is
                    // distinguishable from an empty-string value in Keboola's
                    // untyped (all-string) tables that do not support SQL NULL.
                    buffer_ << kNullSentinel;
                }
                // Non-VARCHAR NULL: write an unquoted empty field.
                // For Keboola typed nullable columns (BIGINT, DOUBLE, …) the
                // Snowflake backend converts unquoted-empty to SQL NULL automatically
                // (EMPTY_FIELD_AS_NULL = TRUE).  For untyped tables the empty string
                // round-trips to an empty string which StringToValue() already
                // converts to a typed NULL via the stoll/stod exception path.
            } else {
                buffer_ << QuoteField(ValueToString(val));
            }
        }
        buffer_ << "\r\n";
        row_count_++;
    }
}

// ---------------------------------------------------------------------------
// GetCsv
// ---------------------------------------------------------------------------

std::string CsvBuilder::GetCsv() const {
    return buffer_.str();
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

void CsvBuilder::Reset() {
    buffer_.str("");
    buffer_.clear();
    row_count_ = 0;
}

} // namespace duckdb
