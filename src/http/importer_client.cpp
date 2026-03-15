#include "http/importer_client.hpp"

// cpp-httplib — bundled with DuckDB in third_party/httplib/httplib.hpp
#include "httplib.hpp"
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace httplib = duckdb_httplib_openssl; // NOLINT
#else
namespace httplib = duckdb_httplib; // NOLINT
#endif

// yyjson is bundled with DuckDB — used to parse the async job response on GCP stacks.
#include "http/yyjson_utils.hpp"

#include "duckdb/common/exception.hpp"

#include <string>
#include <stdexcept>
#include <thread>
#include <chrono>

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

ImporterClient::ImporterClient(const std::string &importer_url,
                               const std::string &storage_url,
                               const std::string &token)
    : importer_url_(importer_url), storage_url_(storage_url), token_(token) {}

// ---------------------------------------------------------------------------
// WriteTable
// ---------------------------------------------------------------------------

int64_t ImporterClient::WriteTable(
    const std::string &table_id,
    const std::string &csv_data,
    bool incremental
) {
    // Build multipart form data per the Keboola Importer API spec.
    // NULL VARCHAR values are encoded as the Unicode PUA sentinel U+E000 (UTF-8 \xEE\x80\x80)
    // in the CSV, and converted back to NULL on the read side by the scanner.
    // Non-VARCHAR NULLs use unquoted empty fields which the Snowflake backend converts to
    // SQL NULL automatically for typed nullable columns (EMPTY_FIELD_AS_NULL = TRUE).
    httplib::UploadFormDataItems items = {
        {"tableId",              table_id,                  "",         ""},
        {"incremental",          incremental ? "1" : "0",   "",         ""},
        {"delimiter",            ",",                        "",         ""},
        {"enclosure",            "\"",                       "",         ""},
        {"escapedBy",            "",                         "",         ""},
        {"data",                 csv_data,                   "data.csv", "text/csv"},
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
    std::string response_body;

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

        response_body = res->body;
        break;
    }

    if (response_body.empty() && !last_error.empty()) {
        throw IOException("Keboola Importer: connection failed after retries: %s", last_error);
    }

    // ---------------------------------------------------------------------------
    // GCP stacks return an async job object from /write-table.
    // Poll it to completion so the caller sees a consistent state.
    // ---------------------------------------------------------------------------
    if (response_body.empty() || storage_url_.empty()) {
        return 0;
    }

    yyjson_read_err yerr;
    yyjson_doc *raw = yyjson_read_opts(const_cast<char *>(response_body.c_str()),
                                       response_body.size(), YYJSON_READ_NOFLAG,
                                       nullptr, &yerr);
    if (!raw) {
        return 0; // not JSON — synchronous response, nothing to poll
    }

    YyjsonDoc d{raw};
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    // Extract job id
    yyjson_val *id_val = yyjson_obj_get(root, "id");
    if (!id_val) return 0;

    std::string job_id;
    if (yyjson_is_int(id_val) || yyjson_is_uint(id_val)) {
        job_id = std::to_string(yyjson_get_sint(id_val));
    } else if (yyjson_is_str(id_val)) {
        const char *s = yyjson_get_str(id_val);
        if (s) job_id = s;
    }

    // Verify it looks like a job (has "status" field)
    yyjson_val *status_val = yyjson_obj_get(root, "status");
    if (!status_val || job_id.empty()) return 0;

    // Poll using a separate client pointed at the Storage API base URL
    httplib::Client poll_cli(storage_url_.c_str());
    poll_cli.set_follow_location(true);
    poll_cli.set_connection_timeout(30);
    poll_cli.set_read_timeout(30);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    poll_cli.enable_server_certificate_verification(true);
#endif

    httplib::Headers poll_headers = {{"X-StorageApi-Token", token_}};

    static constexpr int POLL_TIMEOUT_MS  = 120000;
    static constexpr int POLL_INITIAL_MS  = 100;
    static constexpr double POLL_BACKOFF  = 1.5;
    static constexpr int POLL_MAX_MS      = 2000;

    int elapsed_ms     = 0;
    double interval_ms = static_cast<double>(POLL_INITIAL_MS);

    while (elapsed_ms < POLL_TIMEOUT_MS) {
        int sleep_ms = static_cast<int>(interval_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        elapsed_ms += sleep_ms;

        auto job_res = poll_cli.Get(("/v2/storage/jobs/" + job_id).c_str(), poll_headers);
        if (!job_res || job_res->status < 200 || job_res->status >= 300) {
            // Poll failure — best effort, stop polling
            break;
        }

        yyjson_read_err jerr;
        yyjson_doc *jraw = yyjson_read_opts(const_cast<char *>(job_res->body.c_str()),
                                             job_res->body.size(), YYJSON_READ_NOFLAG,
                                             nullptr, &jerr);
        if (!jraw) break;

        YyjsonDoc jd{jraw};
        yyjson_val *jroot = yyjson_doc_get_root(jd.doc);
        yyjson_val *jstatus_val = yyjson_obj_get(jroot, "status");
        const char *jstatus = jstatus_val ? yyjson_get_str(jstatus_val) : nullptr;

        if (jstatus && std::string(jstatus) == "success") {
            return 0;
        }

        if (jstatus && std::string(jstatus) == "error") {
            yyjson_val *err_val = yyjson_obj_get(jroot, "error");
            if (!err_val) err_val = yyjson_obj_get(jroot, "message");
            const char *err_str = err_val ? yyjson_get_str(err_val) : nullptr;
            throw IOException("Keboola Importer: write-table job %s failed: %s",
                              job_id, err_str ? err_str : "unknown error");
        }

        // "waiting" | "processing" | "running" — keep polling
        interval_ms = std::min(interval_ms * POLL_BACKOFF, static_cast<double>(POLL_MAX_MS));
    }

    return 0;
}

} // namespace duckdb
