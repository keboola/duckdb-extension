#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

//! Client for the Keboola Storage Importer service.
//! Uploads CSV data to a Keboola table via multipart POST.
class ImporterClient {
public:
    //! Construct with the Importer base URL, Storage API base URL, and token.
    //! importer_url: e.g. "https://import.connection.keboola.com"
    //! storage_url:  e.g. "https://connection.us-east4.gcp.keboola.com"
    //!              Used to poll async jobs returned by GCP stacks.
    ImporterClient(const std::string &importer_url,
                   const std::string &storage_url,
                   const std::string &token);

    //! Upload CSV data to a Keboola table via multipart POST to /write-table.
    //! table_id:    e.g. "in.c-crm.contacts"
    //! csv_data:    RFC 4180 CSV with header row
    //! incremental: if true, sends incremental=1 (Keboola deduplicates on PK)
    //! On GCP stacks the endpoint returns an async job; this method polls until
    //! the job completes (or times out) so the caller sees a consistent state.
    //! Returns number of rows written (same as CsvBuilder::RowCount()).
    int64_t WriteTable(
        const std::string &table_id,
        const std::string &csv_data,
        bool incremental = true
    );

private:
    std::string importer_url_;
    std::string storage_url_;
    std::string token_;
};

} // namespace duckdb
