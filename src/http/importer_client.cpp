#include "http/importer_client.hpp"

// cpp-httplib — bundled with DuckDB in third_party/httplib/httplib.hpp
#include "httplib.hpp"
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace httplib = duckdb_httplib_openssl; // NOLINT
#else
namespace httplib = duckdb_httplib; // NOLINT
#endif

#include "duckdb/common/exception.hpp"

#include <string>
#include <stdexcept>
#include <thread>
#include <chrono>

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

ImporterClient::ImporterClient(const std::string &importer_url, const std::string &token)
    : importer_url_(importer_url), token_(token) {}

// ---------------------------------------------------------------------------
// WriteTable
// ---------------------------------------------------------------------------

int64_t ImporterClient::WriteTable(
    const std::string &table_id,
    const std::string &csv_data,
    bool incremental
) {
    // Build multipart form data per the Keboola Importer API spec
    httplib::MultipartFormDataItems items = {
        {"tableId",     table_id,                  "",         ""},
        {"incremental", incremental ? "1" : "0",   "",         ""},
        {"delimiter",   ",",                        "",         ""},
        {"enclosure",   "\"",                       "",         ""},
        {"escapedBy",   "",                         "",         ""},
        {"data",        csv_data,                   "data.csv", "text/csv"},
    };

    // Create httplib client directly (KeboolaHttpClient wrapper doesn't support multipart)
    httplib::Client cli(importer_url_.c_str());
    cli.set_follow_location(true);
    cli.set_connection_timeout(30);
    cli.set_read_timeout(120);
    cli.set_write_timeout(120);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    cli.enable_server_certificate_verification(true);
#endif

    httplib::Headers headers = {
        {"X-StorageApi-Token", token_}
    };

    static constexpr int MAX_RETRIES = 3;
    int delay_ms = 500;
    std::string last_error;

    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            delay_ms *= 2;
        }

        auto res = cli.Post("/write-table", headers, items);

        if (!res) {
            last_error = httplib::to_string(res.error());
            continue;
        }

        // 5xx — retry
        if (res->status >= 500 && res->status < 600) {
            last_error = "HTTP " + std::to_string(res->status) + ": " + res->body;
            continue;
        }

        // 4xx — throw immediately
        if (res->status == 401 || res->status == 403) {
            throw IOException("Keboola Importer: authentication failed (HTTP %d)", res->status);
        }

        if (res->status < 200 || res->status >= 300) {
            throw IOException("Keboola Importer: HTTP error %d: %s",
                              res->status, res->body);
        }

        // Success: response body is not meaningful for row count
        // Row count tracking is done by the caller (CsvBuilder::RowCount())
        return 0;
    }

    throw IOException("Keboola Importer: connection failed after retries: %s", last_error);
}

} // namespace duckdb
