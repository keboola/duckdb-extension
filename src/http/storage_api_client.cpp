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
    // non-copyable
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
        body = http_.Get("/v2/storage/?include=services");
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
        info.id         = JsonStrOr(branch, "id");
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
// ListBuckets
// ---------------------------------------------------------------------------

std::vector<KeboolaBucketInfo> StorageApiClient::ListBuckets() {
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

    size_t bidx, bmax;
    yyjson_val *bucket_json;
    yyjson_arr_foreach(buckets_root, bidx, bmax, bucket_json) {
        KeboolaBucketInfo bucket;
        bucket.id          = JsonStrOr(bucket_json, "id");
        bucket.name        = JsonStrOr(bucket_json, "name");
        bucket.stage       = JsonStrOr(bucket_json, "stage");
        bucket.description = JsonStrOr(bucket_json, "description");
        result.push_back(std::move(bucket));
    }

    // For each bucket, fetch tables with column metadata
    for (auto &bucket : result) {
        std::string tables_body;
        try {
            tables_body = http_.Get("/v2/storage/buckets/" + bucket.id +
                                    "/tables?include=columns,columnMetadata,metadata");
        } catch (...) {
            continue; // non-fatal
        }

        auto td = ParseJson(tables_body, "list-tables-" + bucket.id);
        yyjson_val *tables_root = yyjson_doc_get_root(td.doc);
        if (!yyjson_is_arr(tables_root)) {
            continue;
        }

        size_t tidx, tmax;
        yyjson_val *table_json;
        yyjson_arr_foreach(tables_root, tidx, tmax, table_json) {
            KeboolaTableInfo table;
            table.id        = JsonStrOr(table_json, "id");
            table.name      = JsonStrOr(table_json, "name");
            table.bucket_id = bucket.id;

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

            // Columns array + columnMetadata object
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
                                    kbc_type        = value;
                                    col.keboola_type = value;
                                } else if (key == "KBC.datatype.basetype") {
                                    basetype = value;
                                } else if (key == "KBC.datatype.length") {
                                    // Format: "18,0" or "38,10"
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

            bucket.tables.push_back(std::move(table));
        }
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
            yyjson_val *meta = yyjson_obj_get(ws, "metadata");
            if (meta && yyjson_is_arr(meta)) {
                size_t mi, mm;
                yyjson_val *m;
                yyjson_arr_foreach(meta, mi, mm, m) {
                    std::string key   = JsonStrOr(m, "key");
                    std::string value = JsonStrOr(m, "value");
                    if (key == "created_by" && value == "duckdb-extension") {
                        KeboolaWorkspaceInfo info;
                        info.id   = JsonStrOr(ws, "id");
                        info.name = JsonStrOr(ws, "name");
                        yyjson_val *conn = yyjson_obj_get(ws, "connection");
                        info.type = JsonStrOr(conn, "backend");
                        return info;
                    }
                }
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
    info.id   = JsonStrOr(croot, "id");
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
        http_.Delete("/v2/storage/workspaces/" + workspace_id);
    } catch (...) {
        // Best-effort cleanup — swallow errors
    }
}

// ---------------------------------------------------------------------------
// DeleteRows
// ---------------------------------------------------------------------------

//! URL-encode a string value for use in a query parameter.
//! Encodes space as %20 and all characters outside unreserved set (RFC 3986).
static std::string UrlEncode(const std::string &s) {
    static const char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size() * 3);
    for (unsigned char c : s) {
        if ((c >= 'A' && c <= 'Z') ||
            (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~') {
            out += static_cast<char>(c);
        } else {
            out += '%';
            out += hex[(c >> 4) & 0xF];
            out += hex[c & 0xF];
        }
    }
    return out;
}

int64_t StorageApiClient::DeleteRows(const std::string &table_id,
                                      const KeboolaDeleteParams &params) {
    // Build query-string path:
    //   DELETE /v2/storage/tables/{table_id}/rows
    //     ?deleteWhereColumn={col}
    //     &deleteWhereValues[]={val1}&deleteWhereValues[]={val2}...
    //     &deleteWhereOperator=eq|ne
    //     &allowTruncate=1    (only when allow_truncate)

    std::string path = "/v2/storage/tables/" + table_id + "/rows";

    if (!params.allow_truncate) {
        // Add WHERE clause parameters
        bool first = true;
        auto add = [&](const std::string &key, const std::string &value) {
            path += first ? '?' : '&';
            first = false;
            path += key + "=" + UrlEncode(value);
        };

        add("deleteWhereColumn", params.where_column);

        for (const auto &val : params.where_values) {
            // Use literal [] (PHP array notation) — not percent-encoded
            path += "&deleteWhereValues[]=" + UrlEncode(val);
        }

        if (!params.where_operator.empty()) {
            add("deleteWhereOperator", params.where_operator);
        }
    } else {
        // Plain DELETE (no WHERE) — must send allowTruncate=1
        path += "?allowTruncate=1";
    }

    // Issue the DELETE request — Storage API returns a job object
    std::string resp;
    try {
        resp = http_.Delete(path);
    } catch (const std::exception &e) {
        throw IOException("Keboola Storage API delete-rows failed for table '%s': %s",
                          table_id, std::string(e.what()));
    }

    // Parse the async job id from the response
    auto d = ParseJson(resp, "delete-rows");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    std::string job_id = JsonStrOr(root, "id");
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
            // Try to extract the row count from results.deletedRowsCount
            yyjson_val *results = yyjson_obj_get(jroot, "results");
            if (results && yyjson_is_obj(results)) {
                yyjson_val *cnt = yyjson_obj_get(results, "deletedRowsCount");
                if (cnt) {
                    if (yyjson_is_int(cnt)) {
                        return static_cast<int64_t>(yyjson_get_sint(cnt));
                    } else if (yyjson_is_uint(cnt)) {
                        return static_cast<int64_t>(yyjson_get_uint(cnt));
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

    // Parse the response — it returns the new table object
    auto d = ParseJson(resp, "create-table");
    yyjson_val *root = yyjson_doc_get_root(d.doc);

    KeboolaTableInfo info;
    info.id        = JsonStrOr(root, "id");
    info.name      = JsonStrOr(root, "name");
    info.bucket_id = bucket_id;

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
    try {
        http_.Delete("/v2/storage/buckets/" + bucket_id);
    } catch (const std::exception &e) {
        std::string msg(e.what());
        if (msg.find("HTTP error 404") != std::string::npos ||
            msg.find("HTTP 404") != std::string::npos) {
            throw CatalogException("Schema (bucket) with id \"%s\" not found in Keboola",
                                   bucket_id);
        }
        throw IOException("Keboola Storage API DropBucket failed for bucket '%s': %s",
                          bucket_id, msg);
    }
}

} // namespace duckdb
