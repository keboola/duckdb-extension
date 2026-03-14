#pragma once

#include <string>
#include <stdexcept>
#include <thread>
#include <chrono>

// cpp-httplib — bundled with DuckDB in third_party/httplib/httplib.hpp
// CPPHTTPLIB_OPENSSL_SUPPORT is defined in CMakeLists.txt to enable HTTPS.
// The bundled version uses a renamed namespace to avoid symbol conflicts.
#include "httplib.hpp"
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace httplib = duckdb_httplib_openssl; // NOLINT
#else
namespace httplib = duckdb_httplib; // NOLINT
#endif

namespace duckdb {

//! Lightweight HTTPS client wrapping cpp-httplib.
//! Automatically adds X-StorageApi-Token header and retries on transient errors.
//! Supports https:// URLs (SSL required).
class KeboolaHttpClient {
public:
    //! base_url must include the scheme, e.g. "https://connection.keboola.com"
    KeboolaHttpClient(const std::string &base_url, const std::string &token)
        : base_url_(base_url), token_(token) {}

    //! HTTP GET — returns response body on success, throws on failure.
    std::string Get(const std::string &path) {
        return ExecuteWithRetry([&](httplib::Client &cli) {
            httplib::Headers headers = {{"X-StorageApi-Token", token_}};
            return cli.Get(path.c_str(), headers);
        });
    }

    //! HTTP POST with a body.
    std::string Post(const std::string &path, const std::string &body,
                     const std::string &content_type = "application/json") {
        return ExecuteWithRetry([&](httplib::Client &cli) {
            httplib::Headers headers = {{"X-StorageApi-Token", token_}};
            return cli.Post(path.c_str(), headers, body, content_type.c_str());
        });
    }

    //! HTTP DELETE (no body).
    std::string Delete(const std::string &path) {
        return ExecuteWithRetry([&](httplib::Client &cli) {
            httplib::Headers headers = {{"X-StorageApi-Token", token_}};
            return cli.Delete(path.c_str(), headers);
        });
    }

    //! HTTP DELETE with a JSON body (required by newer Keboola GCP API endpoints).
    std::string Delete(const std::string &path, const std::string &body,
                       const std::string &content_type = "application/json") {
        return ExecuteWithRetry([&](httplib::Client &cli) {
            httplib::Headers headers = {{"X-StorageApi-Token", token_}};
            return cli.Delete(path.c_str(), headers, body, content_type.c_str());
        });
    }

private:
    std::string base_url_;
    std::string token_;

    static constexpr int MAX_RETRIES = 3;

    //! Execute an HTTP operation with exponential-backoff retry on 5xx / network errors.
    //! Fn must accept (httplib::Client&) and return httplib::Result.
    template <typename Fn>
    std::string ExecuteWithRetry(Fn fn) {
        // httplib::Client(url) automatically selects SSL when the URL starts with https://
        httplib::Client cli(base_url_.c_str());
        cli.set_follow_location(true);
        cli.set_connection_timeout(30);
        cli.set_read_timeout(120);  // large bulk responses (e.g. 5000+ tables) may need more time
        cli.set_write_timeout(60);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        // Accept the server's certificate (Keboola uses a valid public CA)
        cli.enable_server_certificate_verification(true);
#endif

        // Exponential backoff delays: 500ms, 1000ms, 2000ms
        static constexpr int BACKOFF_MS[] = {500, 1000, 2000};
        std::string last_error;

        for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
            if (attempt > 0) {
                int delay = BACKOFF_MS[attempt - 1];
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }

            httplib::Result res = fn(cli);

            if (!res) {
                // Network-level error — retry
                last_error = "attempt " + std::to_string(attempt + 1) + "/" +
                             std::to_string(MAX_RETRIES) + " failed: network error: " +
                             httplib::to_string(res.error());
                continue;
            }

            // 429 (rate limit) or 5xx — retry
            if (res->status == 429 ||
                (res->status >= 500 && res->status < 600)) {
                last_error = "attempt " + std::to_string(attempt + 1) + "/" +
                             std::to_string(MAX_RETRIES) + " failed: HTTP " +
                             std::to_string(res->status) + ": " + res->body;
                continue;
            }

            // 4xx — throw immediately (not retriable)
            if (res->status == 401 || res->status == 403) {
                throw std::runtime_error("Keboola authentication failed: invalid token");
            }

            if (res->status < 200 || res->status >= 300) {
                throw std::runtime_error("Keboola HTTP error " + std::to_string(res->status) +
                                         ": " + res->body);
            }

            return res->body;
        }

        throw std::runtime_error("Keboola API: " + last_error);
    }
};

} // namespace duckdb
