#include "http/storage_api_client.hpp"
#include "keboola_delete.hpp"
#include "util/type_mapper.hpp"

#include "duckdb/common/exception.hpp"

// yyjson is bundled with DuckDB and exposed in the duckdb_yyjson namespace.
// The include path resolves through the DuckDB submodule's third_party directory.
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

#include <stdexcept>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <chrono>

namespace duckdb {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

//! RAII wrapper so we never forget to free a yyjson document.
struct YyjsonDoc {
    yyjson_doc *doc = nullptr;
    explicit YyjsonDoc(yyjson_doc *d) : doc(d) {}
    ~YyjsonDoc() {
        if (doc) {
            yyjson_doc_free(doc);
        }
    }
    // movable, non-copyable
    YyjsonDoc(YyjsonDoc &&other) noexcept : doc(other.doc) { other.doc = nullptr; }
    YyjsonDoc &operator=(YyjsonDoc &&other) noexcept {
        if (this != &other) {
            if (doc) yyjson_doc_free(doc);
            doc = other.doc;
            other.doc = nullptr;
        }
        return *this;
    }
    YyjsonDoc(const YyjsonDoc &) = delete;
    YyjsonDoc &operator=(const YyjsonDoc &) = delete;

    yyjson_doc *operator->() const { return doc; }
    bool ok() const { return doc != nullptr; }
};

static YyjsonDoc ParseJson(const std::string &json_str, const std::string &context_hint) {
    yyjson_read_err err;
    yyjson_doc *doc = yyjson_read_opts(const_cast<char *>(json_str.c_str()),
                                       json_str.size(),
                                       YYJSON_READ_NOFLAG,
                                       nullptr,
                                       &err);
    if (!doc) {
        throw IOException("Keboola: failed to parse JSON response (%s): %s",
                          context_hint, err.msg ? err.msg : "unknown error");
    }
    return YyjsonDoc(doc);
}

//! Safe helpers that return a default when the value is null or wrong type.
static std::string JsonStrOr(yyjson_val *obj, const char *key, const char *def = "") {
    if (!obj) return def;
    yyjson_val *v = yyjson_obj_get(obj, key);
    if (!v || !yyjson_is_str(v)) return def;
    return yyjson_get_str(v);
}

static bool JsonBoolOr(yyjson_val *obj, const char *key, bool def = false) {
    if (!obj) return def;
    yyjson_val *v = yyjson_obj_get(obj, key);
    if (!v) return def;
    return yyjson_is_true(v);
}

//! Read a field that may be a JSON string OR a JSON integer (e.g. branch/bucket id).
static std::string JsonIdOr(yyjson_val *obj, const char *key, const char *def = "") {
    if (!obj) return def;
    yyjson_val *v = yyjson_obj_get(obj, key);
    if (!v) return def;
    if (yyjson_is_str(v)) return yyjson_get_str(v);
    if (yyjson_is_int(v))  return std::to_string(yyjson_get_sint(v));
    if (yyjson_is_uint(v)) return std::to_string(yyjson_get_uint(v));
    return def;
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

StorageApiClient::StorageApiClient(const std::string &url, const std::string &token)
    : http_(url, token) {}

// ---------------------------------------------------------------------------
// VerifyAndDiscover
// ---------------------------------------------------------------------------

KeboolaServiceUrls StorageApiClient::VerifyAndDiscover() {
    std::string body;
    try {
        body = http_.Get("/v2/storage");
    } catch (const std::exception &e) {
        std::string msg(e.what());
        if (msg.find("authentication failed") != std::string::npos ||
            msg.find("HTTP 401") != std::string::npos ||
            msg.find("HTTP 403") != std::string::npos) {
            throw IOException("Keboola authentication failed: invalid token");
        }
        throw IOException("Keboola connection failed: %s", msg);
    }

    auto d = ParseJson(body, "verify-and-discover");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    KeboolaServiceUrls urls;
    urls.storage_url = JsonStrOr(root, "url");

    // Parse services array
    yyjson_val *services = yyjson_obj_get(root, "services");
    if (services && yyjson_is_arr(services)) {
        size_t idx, max;
        yyjson_val *svc;
        yyjson_arr_foreach(services, idx, max, svc) {
            std::string svc_id  = JsonStrOr(svc, "id");
            std::string svc_url = JsonStrOr(svc, "url");
            if (svc_id == "query") {
                urls.query_url = svc_url;
            } else if (svc_id == "import") {
                urls.importer_url = svc_url;
            }
        }
    }

    return urls;
}

// ---------------------------------------------------------------------------
// ResolveBranch
// ---------------------------------------------------------------------------

KeboolaBranchInfo StorageApiClient::ResolveBranch(const std::string &branch_name) {
    std::string body;
    try {
        body = http_.Get("/v2/storage/dev-branches/");
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (list branches): %s", std::string(e.what()));
    }

    auto d = ParseJson(body, "list-branches");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    if (!yyjson_is_arr(root)) {
        throw IOException("Keboola: unexpected response for dev-branches");
    }

    KeboolaBranchInfo default_branch;
    size_t idx, max;
    yyjson_val *branch;
    yyjson_arr_foreach(root, idx, max, branch) {
        KeboolaBranchInfo info;
        info.id         = JsonIdOr(branch, "id");
        info.name       = JsonStrOr(branch, "name");
        info.is_default = JsonBoolOr(branch, "isDefault");

        if (info.is_default) {
            default_branch = info;
        }

        if (!branch_name.empty()) {
            if (info.name == branch_name || info.id == branch_name) {
                return info;
            }
        }
    }

    if (!branch_name.empty()) {
        throw IOException("Keboola: branch '%s' not found", branch_name);
    }

    if (default_branch.id.empty()) {
        throw IOException("Keboola: no default branch found");
    }
    return default_branch;
}

// ---------------------------------------------------------------------------
// ParseTableJsonElement — shared by ListBuckets and FetchBucketTables
// ---------------------------------------------------------------------------

static KeboolaTableInfo ParseTableJsonElement(yyjson_val *table_json) {
    KeboolaTableInfo table;
    table.id   = JsonStrOr(table_json, "id");
    table.name = JsonStrOr(table_json, "name");

    // Derive bucket_id: everything before the last '.' in the table id
    auto last_dot = table.id.rfind('.');
    table.bucket_id = (last_dot != std::string::npos)
                      ? table.id.substr(0, last_dot)
                      : table.id;

    // Table-level metadata for description
    yyjson_val *table_meta = yyjson_obj_get(table_json, "metadata");
    if (table_meta && yyjson_is_arr(table_meta)) {
        size_t mi, mm;
        yyjson_val *meta;
        yyjson_arr_foreach(table_meta, mi, mm, meta) {
            std::string mk = JsonStrOr(meta, "key");
            if (mk == "KBC.description") {
                table.description = JsonStrOr(meta, "value");
            }
        }
    }

    // Primary key
    yyjson_val *pk = yyjson_obj_get(table_json, "primaryKey");
    if (pk && yyjson_is_arr(pk)) {
        size_t pi, pm;
        yyjson_val *pv;
        yyjson_arr_foreach(pk, pi, pm, pv) {
            if (yyjson_is_str(pv)) {
                table.primary_key.push_back(yyjson_get_str(pv));
            }
        }
    }

    // Columns + columnMetadata
    yyjson_val *columns_arr  = yyjson_obj_get(table_json, "columns");
    yyjson_val *col_meta_obj = yyjson_obj_get(table_json, "columnMetadata");

    if (columns_arr && yyjson_is_arr(columns_arr)) {
        size_t ci, cm;
        yyjson_val *col_name_val;
        yyjson_arr_foreach(columns_arr, ci, cm, col_name_val) {
            if (!yyjson_is_str(col_name_val)) continue;

            KeboolaColumnInfo col;
            col.name = yyjson_get_str(col_name_val);

            std::string kbc_type;
            std::string basetype;
            int precision = 0;
            int scale     = 0;

            if (col_meta_obj && yyjson_is_obj(col_meta_obj)) {
                yyjson_val *col_meta_arr = yyjson_obj_get(col_meta_obj, col.name.c_str());
                if (col_meta_arr && yyjson_is_arr(col_meta_arr)) {
                    size_t cmi, cmm;
                    yyjson_val *cm_entry;
                    yyjson_arr_foreach(col_meta_arr, cmi, cmm, cm_entry) {
                        std::string key   = JsonStrOr(cm_entry, "key");
                        std::string value = JsonStrOr(cm_entry, "value");

                        if (key == "KBC.datatype.type") {
                            kbc_type         = value;
                            col.keboola_type = value;
                        } else if (key == "KBC.datatype.basetype") {
                            basetype = value;
                        } else if (key == "KBC.datatype.length") {
                            auto comma = value.find(',');
                            if (comma != std::string::npos) {
                                try {
                                    precision = std::stoi(value.substr(0, comma));
                                    scale     = std::stoi(value.substr(comma + 1));
                                } catch (...) {}
                            }
                        } else if (key == "KBC.datatype.nullable") {
                            col.nullable = (value != "0" && value != "false");
                        } else if (key == "KBC.description") {
                            col.description = value;
                        }
                    }
                }
            }

            col.duckdb_type = MapKeboolaTypeToDuckDB(kbc_type, basetype, precision, scale);
            table.columns.push_back(std::move(col));
        }
    }

    return table;
}

// ---------------------------------------------------------------------------
// ListBuckets
// ---------------------------------------------------------------------------

std::vector<KeboolaBucketInfo> StorageApiClient::ListBuckets() {
    // Fetch all buckets in one call
    std::string buckets_body;
    try {
        buckets_body = http_.Get("/v2/storage/buckets");
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (list buckets): %s", std::string(e.what()));
    }

    auto bd = ParseJson(buckets_body, "list-buckets");
    yyjson_val *buckets_root = yyjson_doc_get_root(bd.doc);
    if (!yyjson_is_arr(buckets_root)) {
        throw IOException("Keboola: unexpected response for bucket list");
    }

    std::vector<KeboolaBucketInfo> result;
    // Build an index bucket_id → index into result for O(1) lookup when associating tables
    std::unordered_map<std::string, size_t> bucket_idx;

    size_t bidx, bmax;
    yyjson_val *bucket_json;
    yyjson_arr_foreach(buckets_root, bidx, bmax, bucket_json) {
        KeboolaBucketInfo bucket;
        bucket.id          = JsonStrOr(bucket_json, "id");
        bucket.name        = JsonStrOr(bucket_json, "name");
        bucket.stage       = JsonStrOr(bucket_json, "stage");
        bucket.description = JsonStrOr(bucket_json, "description");
        bucket_idx[bucket.id] = result.size();
        result.push_back(std::move(bucket));
    }

    // Fetch ALL tables in ONE call (avoids N+1 per-bucket requests which is too slow
    // for projects with many buckets, e.g. 500+ buckets → 500+ HTTP round trips).
    std::string tables_body;
    try {
        tables_body = http_.Get("/v2/storage/tables?include=columns,columnMetadata,metadata");
    } catch (const std::exception &e) {
        // Non-fatal: return buckets without table metadata
        return result;
    }

    auto td = ParseJson(tables_body, "list-all-tables");
    yyjson_val *tables_root = yyjson_doc_get_root(td.doc);
    if (!yyjson_is_arr(tables_root)) {
        return result;
    }

    // Associate tables with their buckets
    size_t tidx, tmax;
    yyjson_val *table_json;
    yyjson_arr_foreach(tables_root, tidx, tmax, table_json) {
        KeboolaTableInfo table = ParseTableJsonElement(table_json);
        auto it = bucket_idx.find(table.bucket_id);
        if (it != bucket_idx.end()) {
            result[it->second].tables.push_back(std::move(table));
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// FetchBucketTables
// ---------------------------------------------------------------------------

std::vector<KeboolaTableInfo> StorageApiClient::FetchBucketTables(const std::string &bucket_id) {
    std::string tables_body;
    try {
        tables_body = http_.Get("/v2/storage/buckets/" + bucket_id +
                                "/tables?include=columns,columnMetadata,metadata");
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (fetch bucket tables '%s'): %s",
                          bucket_id, std::string(e.what()));
    }

    auto td = ParseJson(tables_body, "fetch-bucket-tables");
    yyjson_val *tables_root = yyjson_doc_get_root(td.doc);
    if (!yyjson_is_arr(tables_root)) {
        return {};
    }

    std::vector<KeboolaTableInfo> result;
    size_t tidx, tmax;
    yyjson_val *table_json;
    yyjson_arr_foreach(tables_root, tidx, tmax, table_json) {
        result.push_back(ParseTableJsonElement(table_json));
    }
    return result;
}

// ---------------------------------------------------------------------------
// FindOrCreateWorkspace
// ---------------------------------------------------------------------------

KeboolaWorkspaceInfo StorageApiClient::FindOrCreateWorkspace() {
    std::string body;
    try {
        body = http_.Get("/v2/storage/workspaces");
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (list workspaces): %s",
                          std::string(e.what()));
    }

    auto d = ParseJson(body, "list-workspaces");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    if (yyjson_is_arr(root)) {
        size_t idx, max;
        yyjson_val *ws;
        yyjson_arr_foreach(root, idx, max, ws) {
            std::string ws_name = JsonStrOr(ws, "name");
            if (ws_name == "duckdb-extension") {
                KeboolaWorkspaceInfo info;
                info.id   = JsonIdOr(ws, "id");
                info.name = ws_name;
                yyjson_val *conn = yyjson_obj_get(ws, "connection");
                info.type = JsonStrOr(conn, "backend");
                return info;
            }
        }
    }

    // Create a new workspace
    std::string create_body = R"({"name":"duckdb-extension"})";
    std::string create_resp;
    try {
        create_resp = http_.Post("/v2/storage/workspaces", create_body, "application/json");
    } catch (const std::exception &e) {
        throw IOException("Failed to create Keboola workspace: %s", std::string(e.what()));
    }

    auto cd    = ParseJson(create_resp, "create-workspace");
    yyjson_val *croot = yyjson_doc_get_root(cd.doc);

    KeboolaWorkspaceInfo info;
    info.id   = JsonIdOr(croot, "id");
    info.name = JsonStrOr(croot, "name");
    yyjson_val *conn = yyjson_obj_get(croot, "connection");
    info.type = JsonStrOr(conn, "backend");

    if (info.id.empty()) {
        throw IOException("Failed to create Keboola workspace: empty id in response");
    }

    return info;
}

// ---------------------------------------------------------------------------
// DeleteWorkspace
// ---------------------------------------------------------------------------

void StorageApiClient::DeleteWorkspace(const std::string &workspace_id) {
    if (workspace_id.empty()) return;
    try {
        std::string resp = http_.Delete("/v2/storage/workspaces/" + workspace_id);

        // GCP stacks return an async job object; poll it to completion so the
        // workspace is fully deleted before the caller checks.
        if (resp.empty()) return;

        yyjson_read_err err;
        yyjson_doc *raw = yyjson_read_opts(const_cast<char *>(resp.c_str()),
                                           resp.size(), YYJSON_READ_NOFLAG,
                                           nullptr, &err);
        if (!raw) return;
        YyjsonDoc d(raw);
        yyjson_val *root = yyjson_doc_get_root(d.doc);

        std::string job_id = JsonIdOr(root, "id");
        if (job_id.empty()) return;

        // Verify it looks like a job (has "status" field)
        std::string status = JsonStrOr(root, "status");
        if (status.empty()) return;

        // Poll the job until done (best-effort — cap at 30 seconds)
        static constexpr int POLL_TIMEOUT_MS = 30000;
        int elapsed_ms = 0;
        double interval_ms = 200.0;

        while (elapsed_ms < POLL_TIMEOUT_MS) {
            int sleep_ms = static_cast<int>(interval_ms);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            elapsed_ms += sleep_ms;

            std::string job_resp = http_.Get("/v2/storage/jobs/" + job_id);
            yyjson_read_err jerr;
            yyjson_doc *jraw = yyjson_read_opts(const_cast<char *>(job_resp.c_str()),
                                                job_resp.size(), YYJSON_READ_NOFLAG,
                                                nullptr, &jerr);
            if (!jraw) break;
            YyjsonDoc jd(jraw);
            yyjson_val *jroot = yyjson_doc_get_root(jd.doc);
            std::string jstatus = JsonStrOr(jroot, "status");
            if (jstatus == "success" || jstatus == "error") break;

            interval_ms = std::min(interval_ms * 1.5, 2000.0);
        }
    } catch (...) {
        // Best-effort cleanup — swallow errors so DETACH never fails
    }
}

// ---------------------------------------------------------------------------
// DeleteRows
// ---------------------------------------------------------------------------

int64_t StorageApiClient::DeleteRows(const std::string &table_id,
                                      const KeboolaDeleteParams &params) {
    // Storage API DELETE /v2/storage/tables/{table_id}/rows
    //
    // Newer Keboola stacks (GCP) use a JSON body with whereFilters instead of the
    // deprecated deleteWhereColumn / deleteWhereValues[] query parameters:
    //
    //   {"whereFilters":[{"column":"<col>","operator":"eq|ne","values":["v1","v2"]}]}
    //
    // Plain DELETE (no WHERE) sends allowTruncate=1 as a query parameter (still supported).

    std::string path = "/v2/storage/tables/" + table_id + "/rows";

    // Issue the DELETE request — Storage API returns an async job object
    std::string resp;
    try {
        if (params.allow_truncate) {
            // No WHERE clause — full table delete requires allowTruncate=1
            resp = http_.Delete(path + "?allowTruncate=1");
        } else {
            // Build JSON body: {"whereFilters":[{"column":"...","operator":"...","values":["...",...]}]}
            // Manual JSON construction to avoid an extra dependency.
            // Values are JSON-escaped via a minimal helper below.
            auto json_escape = [](const std::string &s) -> std::string {
                std::string out;
                out.reserve(s.size() + 2);
                for (unsigned char c : s) {
                    if (c == '"')       out += "\\\"";
                    else if (c == '\\') out += "\\\\";
                    else if (c == '\n') out += "\\n";
                    else if (c == '\r') out += "\\r";
                    else if (c == '\t') out += "\\t";
                    else if (c < 0x20) {
                        // Other control characters must be escaped as \uXXXX per JSON spec
                        char buf[7];
                        snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(c));
                        out += buf;
                    } else {
                        out += static_cast<char>(c);
                    }
                }
                return out;
            };

            std::string op = params.where_operator.empty() ? "eq" : params.where_operator;
            std::string body = "{\"whereFilters\":[{\"column\":\"" + json_escape(params.where_column) +
                               "\",\"operator\":\"" + json_escape(op) + "\",\"values\":[";
            for (std::size_t i = 0; i < params.where_values.size(); ++i) {
                if (i > 0) body += ",";
                body += "\"" + json_escape(params.where_values[i]) + "\"";
            }
            body += "]}]}";

            resp = http_.Delete(path, body, "application/json");
        }
    } catch (const std::exception &e) {
        throw IOException("Keboola Storage API delete-rows failed for table '%s': %s",
                          table_id, std::string(e.what()));
    }

    // Parse the async job id from the response
    auto d = ParseJson(resp, "delete-rows");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    std::string job_id = JsonIdOr(root, "id");
    if (job_id.empty()) {
        // Some API versions return 204 with empty body or immediate status
        // Check if there's a "status" field indicating the job is already done
        std::string status = JsonStrOr(root, "status");
        if (status == "success") {
            return -1; // count unavailable
        }
        // If there's no job id and no success status, the delete may have
        // been processed synchronously (no job to poll).
        return -1;
    }

    // Poll the job until it completes
    static constexpr int POLL_INITIAL_MS  = 100;
    static constexpr double POLL_BACKOFF  = 1.5;
    static constexpr int POLL_MAX_MS      = 2000;
    static constexpr int POLL_TIMEOUT_MS  = 120000;

    int elapsed_ms      = 0;
    double interval_ms  = static_cast<double>(POLL_INITIAL_MS);

    while (elapsed_ms < POLL_TIMEOUT_MS) {
        int sleep_ms = static_cast<int>(interval_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        elapsed_ms += sleep_ms;

        std::string job_resp;
        try {
            job_resp = http_.Get("/v2/storage/jobs/" + job_id);
        } catch (const std::exception &e) {
            throw IOException(
                "Keboola Storage API: poll failed for delete job %s: %s",
                job_id, std::string(e.what()));
        }

        auto jd = ParseJson(job_resp, "poll-delete-job");
        yyjson_val *jroot = yyjson_doc_get_root(jd.doc);

        std::string status = JsonStrOr(jroot, "status");

        if (status == "success") {
            // Try to extract the row count from results.deletedRowsCount.
            // The GCP API may return deletedRowsCount as an integer or as a
            // JSON string (e.g. "2") — handle both.
            yyjson_val *results = yyjson_obj_get(jroot, "results");
            if (results && yyjson_is_obj(results)) {
                yyjson_val *cnt = yyjson_obj_get(results, "deletedRowsCount");
                if (cnt) {
                    if (yyjson_is_int(cnt)) {
                        return static_cast<int64_t>(yyjson_get_sint(cnt));
                    } else if (yyjson_is_uint(cnt)) {
                        return static_cast<int64_t>(yyjson_get_uint(cnt));
                    } else if (yyjson_is_real(cnt)) {
                        return static_cast<int64_t>(yyjson_get_real(cnt));
                    } else if (yyjson_is_str(cnt)) {
                        const char *s = yyjson_get_str(cnt);
                        if (s) {
                            try { return static_cast<int64_t>(std::stoll(s)); }
                            catch (...) {}
                        }
                    }
                }
            }
            return -1; // success but count not in response
        }

        if (status == "error") {
            std::string err = JsonStrOr(jroot, "error");
            if (err.empty()) {
                err = JsonStrOr(jroot, "message");
            }
            if (err.empty()) {
                err = "unknown error";
            }
            throw IOException("Keboola Storage API: delete job %s failed: %s",
                              job_id, err);
        }

        // "waiting" | "processing" | "running" — keep polling
        interval_ms *= POLL_BACKOFF;
        if (interval_ms > static_cast<double>(POLL_MAX_MS)) {
            interval_ms = static_cast<double>(POLL_MAX_MS);
        }
    }

    throw IOException(
        "Keboola Storage API: delete job %s timed out after %d seconds",
        job_id, POLL_TIMEOUT_MS / 1000);
}

// ---------------------------------------------------------------------------
// Phase 6: DDL — CreateTable
// ---------------------------------------------------------------------------

KeboolaTableInfo StorageApiClient::CreateTable(
    const std::string &bucket_id,
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_defs) {

    // Build JSON body for POST /v2/storage/buckets/{bucket_id}/tables-definition
    // {
    //   "name": "table_name",
    //   "primaryKeysNames": [],
    //   "columns": [
    //     {"name": "col1", "definition": {"type": "STRING"}},
    //     ...
    //   ]
    // }

    // Map DuckDB types to Keboola native types for the definition API
    auto duck_to_kbc_type = [](const std::string &duckdb_type) -> std::string {
        const auto t = [&]() {
            std::string u = duckdb_type;
            for (char &c : u) c = static_cast<char>(::toupper(static_cast<unsigned char>(c)));
            return u;
        }();
        if (t == "BIGINT" || t == "INTEGER" || t == "SMALLINT" || t == "TINYINT" ||
            t == "INT" || t == "INT8" || t == "INT4" || t == "INT2") return "INTEGER";
        if (t == "DOUBLE" || t == "FLOAT" || t == "REAL" || t == "FLOAT8" || t == "FLOAT4")
            return "FLOAT";
        if (t == "BOOLEAN" || t == "BOOL") return "BOOLEAN";
        if (t == "DATE") return "DATE";
        if (t == "TIMESTAMP" || t == "DATETIME") return "TIMESTAMP";
        if (t == "TIMESTAMPTZ" || t == "TIMESTAMP WITH TIME ZONE") return "TIMESTAMP";
        // DECIMAL(p,s) and everything else: STRING
        return "STRING";
    };

    std::string body = "{\"name\":\"";
    // Escape table name (basic — assume it's alphanumeric)
    body += table_name;
    body += "\",\"primaryKeysNames\":[],\"columns\":[";

    for (size_t i = 0; i < column_defs.size(); ++i) {
        if (i > 0) body += ",";
        body += "{\"name\":\"";
        body += column_defs[i].first;
        body += "\",\"definition\":{\"type\":\"";
        body += duck_to_kbc_type(column_defs[i].second);
        body += "\"}}";
    }
    body += "]}";

    std::string resp;
    try {
        resp = http_.Post("/v2/storage/buckets/" + bucket_id + "/tables-definition",
                          body, "application/json");
    } catch (const std::exception &e) {
        throw IOException("Keboola Storage API CreateTable failed for bucket '%s', table '%s': %s",
                          bucket_id, table_name, std::string(e.what()));
    }

    // Parse the response.
    // On newer Keboola stacks (GCP), tables-definition returns a 202 async job:
    //   {"id": <job_id>, "status": "waiting", "tableId": "stage.c-bucket.table_name", ...}
    // On older stacks it may return the table object directly.
    auto d = ParseJson(resp, "create-table");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    KeboolaTableInfo info;
    info.bucket_id = bucket_id;

    // Detect async job by presence of "status" field.
    // On newer Keboola stacks (GCP), tables-definition returns a 202 job response.
    yyjson_val *status_val = yyjson_obj_get(root, "status");
    if (status_val) {
        // Extract job ID and poll until completion
        std::string job_id = JsonIdOr(root, "id");

        if (!job_id.empty()) {
            static constexpr int POLL_INITIAL_MS = 100;
            static constexpr double POLL_BACKOFF = 1.5;
            static constexpr int POLL_MAX_MS     = 2000;
            static constexpr int POLL_TIMEOUT_MS = 60000;

            int elapsed_ms     = 0;
            double interval_ms = static_cast<double>(POLL_INITIAL_MS);

            while (elapsed_ms < POLL_TIMEOUT_MS) {
                int sleep_ms = static_cast<int>(interval_ms);
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                elapsed_ms += sleep_ms;

                std::string job_resp;
                try {
                    job_resp = http_.Get("/v2/storage/jobs/" + job_id);
                } catch (const std::exception &e) {
                    throw IOException(
                        "Keboola CreateTable: poll failed for job %s: %s",
                        job_id, std::string(e.what()));
                }

                auto jd = ParseJson(job_resp, "poll-create-table-job");
                yyjson_val *jroot = yyjson_doc_get_root(jd.doc);
                std::string jstatus = JsonStrOr(jroot, "status");

                if (jstatus == "success") {
                    break; // Table created — proceed to construct info below
                }
                if (jstatus == "error") {
                    std::string err = JsonStrOr(jroot, "error");
                    if (err.empty()) err = JsonStrOr(jroot, "message");
                    if (err.empty()) err = "unknown error";
                    throw IOException("Keboola CreateTable: job %s failed: %s",
                                      job_id, err);
                }

                interval_ms *= POLL_BACKOFF;
                if (interval_ms > static_cast<double>(POLL_MAX_MS)) {
                    interval_ms = static_cast<double>(POLL_MAX_MS);
                }
            }
        }

        // Use "tableId" from the job response if available and non-empty,
        // else construct from inputs (GCP async response may have tableId = "")
        yyjson_val *table_id_val = yyjson_obj_get(root, "tableId");
        const char *table_id_cstr = (table_id_val && yyjson_is_str(table_id_val))
                                     ? yyjson_get_str(table_id_val) : nullptr;
        if (table_id_cstr && table_id_cstr[0] != '\0') {
            info.id = table_id_cstr;
        } else {
            info.id = bucket_id + "." + table_name;
        }
        info.name = table_name;
    } else {
        // Synchronous response — parse as table object
        info.id   = JsonStrOr(root, "id");
        info.name = JsonStrOr(root, "name");
    }

    // Parse columns from the definition response
    yyjson_val *cols_arr = yyjson_obj_get(root, "columns");
    if (cols_arr && yyjson_is_arr(cols_arr)) {
        size_t ci, cm;
        yyjson_val *col_val;
        yyjson_arr_foreach(cols_arr, ci, cm, col_val) {
            if (!yyjson_is_str(col_val)) continue;
            KeboolaColumnInfo col;
            col.name = yyjson_get_str(col_val);
            // Find matching type from our input column_defs
            for (const auto &cd : column_defs) {
                if (cd.first == col.name) {
                    col.duckdb_type = cd.second;
                    break;
                }
            }
            if (col.duckdb_type.empty()) col.duckdb_type = "VARCHAR";
            info.columns.push_back(std::move(col));
        }
    }

    // If columns array was empty/missing, reconstruct from our input
    if (info.columns.empty()) {
        for (const auto &cd : column_defs) {
            KeboolaColumnInfo col;
            col.name        = cd.first;
            col.duckdb_type = cd.second;
            info.columns.push_back(std::move(col));
        }
    }

    return info;
}

// ---------------------------------------------------------------------------
// Phase 6: DDL — DropTable
// ---------------------------------------------------------------------------

void StorageApiClient::DropTable(const std::string &table_id) {
    try {
        http_.Delete("/v2/storage/tables/" + table_id);
    } catch (const std::exception &e) {
        std::string msg(e.what());
        // 404 → table not found → translate to CatalogException message
        if (msg.find("HTTP error 404") != std::string::npos ||
            msg.find("HTTP 404") != std::string::npos) {
            throw CatalogException("Table with id \"%s\" not found in Keboola", table_id);
        }
        throw IOException("Keboola Storage API DropTable failed for table '%s': %s",
                          table_id, msg);
    }
}

// ---------------------------------------------------------------------------
// Phase 6: DDL — CreateBucket
// ---------------------------------------------------------------------------

KeboolaBucketInfo StorageApiClient::CreateBucket(const std::string &stage,
                                                   const std::string &name,
                                                   const std::string &description) {
    // POST /v2/storage/buckets
    // body: stage=in&name=c-myapp&description=...
    // The Storage API for bucket creation accepts form-encoded params
    std::string body = "stage=" + stage + "&name=" + name;
    if (!description.empty()) {
        body += "&description=" + description;
    }

    std::string resp;
    try {
        resp = http_.Post("/v2/storage/buckets", body, "application/x-www-form-urlencoded");
    } catch (const std::exception &e) {
        throw IOException("Keboola Storage API CreateBucket failed for '%s.%s': %s",
                          stage, name, std::string(e.what()));
    }

    auto d = ParseJson(resp, "create-bucket");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    KeboolaBucketInfo info;
    info.id          = JsonStrOr(root, "id");
    info.name        = JsonStrOr(root, "name");
    info.stage       = JsonStrOr(root, "stage");
    info.description = JsonStrOr(root, "description");

    // If id is empty, construct it from stage+name (fallback)
    if (info.id.empty()) {
        info.id    = stage + "." + name;
        info.name  = name;
        info.stage = stage;
    }

    return info;
}

// ---------------------------------------------------------------------------
// Phase 6: DDL — DropBucket
// ---------------------------------------------------------------------------

void StorageApiClient::DropBucket(const std::string &bucket_id) {
    // GCP stacks require async=1; older stacks accept synchronous delete.
    // Try async first (force+async), then fall back to sync if 4xx suggests it is not supported.
    std::string resp;
    bool is_async = false;
    try {
        resp = http_.Delete("/v2/storage/buckets/" + bucket_id + "?force=1&async=1");
        is_async = true;
    } catch (const std::exception &e) {
        std::string msg(e.what());
        if (msg.find("HTTP error 404") != std::string::npos ||
            msg.find("HTTP 404") != std::string::npos) {
            throw CatalogException("Schema (bucket) with id \"%s\" not found in Keboola",
                                   bucket_id);
        }
        // If async is not supported, fall back to synchronous delete
        if (msg.find("HTTP error 4") != std::string::npos) {
            try {
                http_.Delete("/v2/storage/buckets/" + bucket_id);
                return; // sync delete succeeded
            } catch (const std::exception &e2) {
                throw IOException("Keboola Storage API DropBucket failed for bucket '%s': %s",
                                  bucket_id, std::string(e2.what()));
            }
        }
        throw IOException("Keboola Storage API DropBucket failed for bucket '%s': %s",
                          bucket_id, msg);
    }

    if (!is_async || resp.empty()) return;

    // Poll the async job to completion
    auto d = ParseJson(resp, "drop-bucket-job");
    yyjson_val *root = yyjson_doc_get_root(d.doc);
    std::string job_id = JsonIdOr(root, "id");
    if (job_id.empty()) return;

    static constexpr int POLL_INITIAL_MS = 100;
    static constexpr double POLL_BACKOFF = 1.5;
    static constexpr int POLL_MAX_MS     = 2000;
    static constexpr int POLL_TIMEOUT_MS = 60000;

    int elapsed_ms     = 0;
    double interval_ms = static_cast<double>(POLL_INITIAL_MS);

    while (elapsed_ms < POLL_TIMEOUT_MS) {
        int sleep_ms = static_cast<int>(interval_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        elapsed_ms += sleep_ms;

        std::string job_resp;
        try {
            job_resp = http_.Get("/v2/storage/jobs/" + job_id);
        } catch (const std::exception &e) {
            throw IOException("Keboola DropBucket: poll failed for job %s: %s",
                              job_id, std::string(e.what()));
        }

        auto jd = ParseJson(job_resp, "poll-drop-bucket-job");
        yyjson_val *jroot = yyjson_doc_get_root(jd.doc);
        std::string jstatus = JsonStrOr(jroot, "status");
        if (jstatus == "success") return;
        if (jstatus == "error") {
            std::string err = JsonStrOr(jroot, "error");
            if (err.empty()) err = JsonStrOr(jroot, "message");
            if (err.empty()) err = "unknown error";
            throw IOException("Keboola DropBucket: job %s failed: %s", job_id, err);
        }
        interval_ms *= POLL_BACKOFF;
        if (interval_ms > static_cast<double>(POLL_MAX_MS)) {
            interval_ms = static_cast<double>(POLL_MAX_MS);
        }
    }
    throw IOException("Keboola DropBucket: job %s did not complete within %d seconds",
                      job_id, POLL_TIMEOUT_MS / 1000);
}

} // namespace duckdb
