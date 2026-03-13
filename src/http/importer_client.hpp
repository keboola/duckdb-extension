#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

//! Client for the Keboola Storage Importer service.
//! Uploads CSV data to a Keboola table via multipart POST.
class ImporterClient {
public:
    //! Construct with the Importer base URL and Storage API token.
    //! importer_url: e.g. "https://import.connection.keboola.com"
    ImporterClient(const std::string &importer_url, const std::string &token);

    //! Upload CSV data to a Keboola table via multipart POST to /write-table.
    //! table_id:    e.g. "in.c-crm.contacts"
    //! csv_data:    RFC 4180 CSV with header row
    //! incremental: if true, sends incremental=1 (Keboola deduplicates on PK)
    //! Returns number of rows written (same as CsvBuilder::RowCount()).
    int64_t WriteTable(
        const std::string &table_id,
        const std::string &csv_data,
        bool incremental = true
    );

private:
    std::string importer_url_;
    std::string token_;
};

} // namespace duckdb
