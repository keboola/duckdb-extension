#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"

#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

//! RFC 4180 compliant CSV generator.
//! Accumulates rows from DuckDB DataChunks and produces a CSV string.
class CsvBuilder {
public:
    CsvBuilder() : row_count_(0) {}

    //! Add a header row from column names.
    void AddHeader(const std::vector<std::string> &column_names);

    //! Add a row of string values (NULLs represented as empty string).
    void AddRow(const std::vector<std::string> &values);

    //! Add rows directly from a DuckDB DataChunk.
    //! column_names: names for all columns in the chunk (unused — only used for documentation).
    void AddChunk(const DataChunk &chunk, const std::vector<std::string> &column_names);

    //! Get the complete CSV as a string.
    std::string GetCsv() const;

    //! Reset for reuse.
    void Reset();

    //! Row count (excluding header).
    idx_t RowCount() const { return row_count_; }

private:
    std::ostringstream buffer_;
    idx_t row_count_ = 0;

    //! RFC 4180: field needs quoting if it contains comma, double-quote, newline, or CR.
    static std::string QuoteField(const std::string &field);

    //! Convert a DuckDB Value to string for CSV.
    static std::string ValueToString(const Value &val);
};

} // namespace duckdb
