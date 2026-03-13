#include "http/query_service_client.hpp"

#include "duckdb/common/exception.hpp"

#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

#include <chrono>
#include <thread>
#include <stdexcept>
#include <string>

namespace duckdb {

// ---------------------------------------------------------------------------
// RAII yyjson document wrapper (same pattern as storage_api_client.cpp)
// ---------------------------------------------------------------------------

struct QSYyjsonDoc {
    yyjson_doc *doc = nullptr;
    explicit QSYyjsonDoc(yyjson_doc *d) : doc(d) {}
    ~QSYyjsonDoc() {
        if (doc) {
            yyjson_doc_free(doc);
        }
    }
    // movable, non-copyable
    QSYyjsonDoc(QSYyjsonDoc &&other) noexcept : doc(other.doc) { other.doc = nullptr; }
    QSYyjsonDoc &operator=(QSYyjsonDoc &&other) noexcept {
        if (this != &other) {
            if (doc) yyjson_doc_free(doc);
            doc = other.doc;
            other.doc = nullptr;
        }
        return *this;
    }
    QSYyjsonDoc(const QSYyjsonDoc &) = delete;
    QSYyjsonDoc &operator=(const QSYyjsonDoc &) = delete;
    bool ok() const { return doc != nullptr; }
};

static QSYyjsonDoc QSParseJson(const std::string &json_str, const std::string &ctx) {
    yyjson_read_err err;
    yyjson_doc *doc = yyjson_read_opts(const_cast<char *>(json_str.c_str()),
                                       json_str.size(),
                                       YYJSON_READ_NOFLAG,
                                       nullptr,
                                       &err);
    if (!doc) {
        throw IOException("Keboola QueryService: failed to parse JSON (%s): %s",
                          ctx, err.msg ? err.msg : "unknown error");
    }
    return QSYyjsonDoc(doc);
}

static std::string QSStrOr(yyjson_val *obj, const char *key, const char *def = "") {
    if (!obj) return def;
    yyjson_val *v = yyjson_obj_get(obj, key);
    if (!v || !yyjson_is_str(v)) return def;
    return yyjson_get_str(v);
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

QueryServiceClient::QueryServiceClient(const std::string &query_service_url,
                                       const std::string &storage_token,
                                       const std::string &branch_id,
                                       const std::string &workspace_id)
    : http_(query_service_url, storage_token),
      branch_id_(branch_id),
      workspace_id_(workspace_id) {}

// ---------------------------------------------------------------------------
// ExecuteQuery — public entry point
// ---------------------------------------------------------------------------

QueryServiceResult QueryServiceClient::ExecuteQuery(const std::string &sql) {
    auto job_id = SubmitQuery(sql);
    PollUntilDone(job_id);
    return FetchResults(job_id);
}

// ---------------------------------------------------------------------------
// SubmitQuery
// ---------------------------------------------------------------------------

std::string QueryServiceClient::SubmitQuery(const std::string &sql) {
    // Escape double quotes in SQL for JSON embedding
    std::string escaped_sql;
    escaped_sql.reserve(sql.size());
    for (char c : sql) {
        if (c == '"') {
            escaped_sql += "\\\"";
        } else if (c == '\\') {
            escaped_sql += "\\\\";
        } else if (c == '\n') {
            escaped_sql += "\\n";
        } else if (c == '\r') {
            escaped_sql += "\\r";
        } else if (c == '\t') {
            escaped_sql += "\\t";
        } else {
            escaped_sql += c;
        }
    }

    std::string body = "{\"sql\":\"" + escaped_sql + "\",\"parameters\":[]}";
    std::string path = "/api/v1/branches/" + branch_id_ +
                       "/workspaces/" + workspace_id_ + "/queries";

    std::string resp;
    try {
        resp = http_.Post(path, body, "application/json");
    } catch (const std::exception &e) {
        throw IOException("Keboola QueryService: failed to submit query: %s", std::string(e.what()));
    }

    auto d = QSParseJson(resp, "submit-query");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    std::string job_id = QSStrOr(root, "id");
    if (job_id.empty()) {
        throw IOException("Keboola QueryService: no job id in submit response");
    }
    return job_id;
}

// ---------------------------------------------------------------------------
// PollUntilDone — exponential backoff
// ---------------------------------------------------------------------------

void QueryServiceClient::PollUntilDone(const std::string &job_id) {
    std::string path = "/api/v1/queries/" + job_id;

    int elapsed_ms = 0;
    double interval_ms = static_cast<double>(POLL_INITIAL_MS);

    while (elapsed_ms < POLL_TIMEOUT_MS) {
        int sleep_ms = static_cast<int>(interval_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        elapsed_ms += sleep_ms;

        std::string resp;
        try {
            resp = http_.Get(path);
        } catch (const std::exception &e) {
            throw IOException("Keboola QueryService: poll failed for job %s: %s",
                              job_id, std::string(e.what()));
        }

        auto d = QSParseJson(resp, "poll-query");
        yyjson_val *root = yyjson_doc_get_root(d.doc);

        std::string status = QSStrOr(root, "status");

        if (status == "success") {
            return;
        } else if (status == "error") {
            std::string error_msg = QSStrOr(root, "error");
            if (error_msg.empty()) {
                error_msg = QSStrOr(root, "message");
            }
            if (error_msg.empty()) {
                error_msg = "unknown error";
            }
            throw IOException("Keboola QueryService: query failed: %s", error_msg);
        } else if (status == "processing" || status == "waiting" || status == "running") {
            // keep polling
        } else if (!status.empty()) {
            // Unknown status — keep polling conservatively
        }

        // Advance backoff
        interval_ms *= POLL_BACKOFF;
        if (interval_ms > static_cast<double>(POLL_MAX_MS)) {
            interval_ms = static_cast<double>(POLL_MAX_MS);
        }
    }

    throw IOException("Keboola QueryService: query timed out after %d seconds (job %s)",
                      POLL_TIMEOUT_MS / 1000, job_id);
}

// ---------------------------------------------------------------------------
// FetchResults — paginated
// ---------------------------------------------------------------------------

QueryServiceResult QueryServiceClient::FetchResults(const std::string &job_id) {
    QueryServiceResult result;
    bool columns_populated = false;
    int64_t offset = 0;

    while (true) {
        std::string path = "/api/v1/queries/" + job_id + "/results"
                           "?offset=" + std::to_string(offset) +
                           "&limit=" + std::to_string(QUERY_PAGE_SIZE);

        std::string resp;
        try {
            resp = http_.Get(path);
        } catch (const std::exception &e) {
            throw IOException("Keboola QueryService: failed to fetch results (offset=%lld): %s",
                              static_cast<long long>(offset), std::string(e.what()));
        }

        auto d = QSParseJson(resp, "fetch-results");
        yyjson_val *root = yyjson_doc_get_root(d.doc);

        // Parse columns on first page
        if (!columns_populated) {
            yyjson_val *cols = yyjson_obj_get(root, "columns");
            if (cols && yyjson_is_arr(cols)) {
                size_t ci, cm;
                yyjson_val *col;
                yyjson_arr_foreach(cols, ci, cm, col) {
                    QueryServiceColumn qsc;
                    qsc.name = QSStrOr(col, "name");
                    qsc.type = QSStrOr(col, "type");
                    result.columns.push_back(std::move(qsc));
                }
            }

            // Parse totalRowCount
            yyjson_val *total_val = yyjson_obj_get(root, "totalRowCount");
            if (total_val && yyjson_is_int(total_val)) {
                result.total_rows = static_cast<int64_t>(yyjson_get_int(total_val));
            } else if (total_val && yyjson_is_uint(total_val)) {
                result.total_rows = static_cast<int64_t>(yyjson_get_uint(total_val));
            }

            columns_populated = true;
        }

        // Parse rows
        yyjson_val *rows = yyjson_obj_get(root, "rows");
        int64_t page_row_count = 0;
        if (rows && yyjson_is_arr(rows)) {
            size_t ri, rm;
            yyjson_val *row;
            yyjson_arr_foreach(rows, ri, rm, row) {
                if (!yyjson_is_arr(row)) continue;

                std::vector<std::string> row_values;
                std::vector<bool> row_nulls;

                size_t ci, cm;
                yyjson_val *cell;
                yyjson_arr_foreach(row, ci, cm, cell) {
                    if (yyjson_is_null(cell)) {
                        row_values.push_back("");
                        row_nulls.push_back(true);
                    } else if (yyjson_is_str(cell)) {
                        row_values.push_back(yyjson_get_str(cell));
                        row_nulls.push_back(false);
                    } else if (yyjson_is_int(cell)) {
                        row_values.push_back(std::to_string(yyjson_get_sint(cell)));
                        row_nulls.push_back(false);
                    } else if (yyjson_is_uint(cell)) {
                        row_values.push_back(std::to_string(yyjson_get_uint(cell)));
                        row_nulls.push_back(false);
                    } else if (yyjson_is_real(cell)) {
                        row_values.push_back(std::to_string(yyjson_get_real(cell)));
                        row_nulls.push_back(false);
                    } else if (yyjson_is_bool(cell)) {
                        row_values.push_back(yyjson_is_true(cell) ? "true" : "false");
                        row_nulls.push_back(false);
                    } else {
                        row_values.push_back("");
                        row_nulls.push_back(true);
                    }
                }

                result.rows.push_back(std::move(row_values));
                result.null_mask.push_back(std::move(row_nulls));
                page_row_count++;
            }
        }

        offset += page_row_count;

        // Determine if there are more pages
        // Use totalRowCount if available, otherwise check if we got a full page
        if (result.total_rows > 0) {
            if (offset >= result.total_rows) {
                break;
            }
        } else {
            // No totalRowCount — stop when we got less than a full page
            if (page_row_count < QUERY_PAGE_SIZE) {
                break;
            }
        }

        // Safety: if this page was empty, stop to avoid infinite loop
        if (page_row_count == 0) {
            break;
        }
    }

    result.has_more = false;
    return result;
}

} // namespace duckdb
