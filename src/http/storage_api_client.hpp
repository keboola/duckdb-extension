#pragma once

#include "http/http_client.hpp"
#include <string>
#include <vector>
#include <memory>

namespace duckdb {

// Forward declaration — full definition lives in keboola_delete.hpp
struct KeboolaDeleteParams;

//! Service endpoint URLs discovered from the Storage API token verification response.
struct KeboolaServiceUrls {
    std::string storage_url;   //!< Base URL of this stack's Storage API
    std::string query_url;     //!< URL of the Query (Workspace Query) service
    std::string importer_url;  //!< URL of the Importer service
};

//! Information about a Keboola dev branch.
struct KeboolaBranchInfo {
    std::string id;
    std::string name;
    bool is_default = false;
};

//! Column metadata for a single Keboola table column.
struct KeboolaColumnInfo {
    std::string name;
    std::string duckdb_type;   //!< Mapped DuckDB type string, e.g. "BIGINT", "VARCHAR"
    std::string keboola_type;  //!< Raw Keboola type as returned by the API
    bool nullable = true;
    std::string description;   //!< From KBC.description metadata
};

//! Metadata for a single Keboola table.
struct KeboolaTableInfo {
    std::string id;            //!< e.g. "in.c-crm.contacts"
    std::string name;          //!< e.g. "contacts"
    std::string bucket_id;     //!< e.g. "in.c-crm"
    std::string description;   //!< From KBC.description metadata
    std::vector<std::string> primary_key;
    std::vector<KeboolaColumnInfo> columns;

    //! Copy of the parent bucket's link/Snowflake metadata.  Populated during
    //! catalog discovery so SQL generators don't need to look up the bucket.
    bool is_linked = false;
    std::string sf_database;   //!< e.g. "KBC_USE4_5118" (own) or "KBC_USE4_340" (linked)
    std::string sf_schema;     //!< e.g. "in.c-crm" (own) or "out.c-source_bucket" (linked)
};

//! Metadata for a Keboola bucket (maps to a DuckDB schema).
struct KeboolaBucketInfo {
    std::string id;            //!< e.g. "in.c-crm" (Keboola-side bucket identifier)
    std::string name;          //!< e.g. "c-crm"
    std::string stage;         //!< "in" or "out"
    std::string description;
    std::vector<KeboolaTableInfo> tables;

    //! True if this is a linked bucket (read-only, owned by another project).
    //! Linked buckets cannot be written to via the Storage Importer API; the
    //! source project must do that.  For SELECT, the SQL must reference the
    //! source project's Snowflake DB + bucket path because the linked bucket
    //! does NOT materialize as a schema in the local project's database.
    bool is_linked = false;

    //! Snowflake-side fully-qualified location of this bucket's data.
    //! From the Storage API "backendPath" field: [database, schema].
    //!  - For own buckets: e.g. ["KBC_USE4_5118", "in.c-mybucket"]
    //!  - For linked buckets: ["KBC_USE4_<sourceProject>", "<sourceBucketPath>"]
    //! Empty when the API doesn't expose backendPath; SQL falls back to using
    //! the bucket id as an unqualified schema (works only for own buckets in
    //! the workspace's local database).
    std::string sf_database;
    std::string sf_schema;
};

//! Metadata for a Keboola workspace.
struct KeboolaWorkspaceInfo {
    std::string id;
    std::string name;
    std::string type;          //!< "snowflake", "bigquery", etc.
};

//! High-level Keboola Storage API client.
class StorageApiClient {
public:
    //! Construct with the stack URL (e.g. "https://connection.keboola.com") and API token.
    explicit StorageApiClient(const std::string &url, const std::string &token);

    //! Verify the token and discover service URLs.
    //! Throws IOException on auth failure or network error.
    KeboolaServiceUrls VerifyAndDiscover();

    //! Resolve a branch by name/ID.  If branch_name is empty, returns the default branch.
    KeboolaBranchInfo ResolveBranch(const std::string &branch_name = "");

    //! List all buckets visible to the token, including tables and column metadata.
    std::vector<KeboolaBucketInfo> ListBuckets();

    //! Fetch all tables for a single bucket, including column metadata.
    //! Faster than ListBuckets() when only one bucket needs refreshing.
    std::vector<KeboolaTableInfo> FetchBucketTables(const std::string &bucket_id);

    //! Return the project's default storage backend ("snowflake", "bigquery",
    //! …) from the token-verify response, or "" when unavailable.
    std::string GetDefaultBackend();

    //! Create a new workspace with a unique session-specific name.
    //! Also garbage-collects stale workspaces from previous crashed sessions.
    //!
    //! When read_only_storage_access is true, the workspace's Snowflake role
    //! sees every storage bucket the token can read as a schema, so the
    //! Query Service can resolve table references like "in.c-bucket"."tbl"
    //! without explicit per-table loading.  When false, the workspace is an
    //! empty Snowflake schema with no view of storage buckets.
    //!
    //! On Snowflake projects the workspace is requested with
    //! loginType=snowflake-service-keypair and a throwaway public key, because
    //! Snowflake is removing the LEGACY_SERVICE user type the platform would
    //! otherwise default to. Stacks that don't support the parameter get the
    //! legacy request as a fallback.
    KeboolaWorkspaceInfo CreateSessionWorkspace(bool read_only_storage_access = true);

    //! Delete stale "duckdb-ext-*" workspaces that were orphaned by crashed sessions.
    //! Best-effort: errors are silently ignored.
    void CleanupStaleWorkspaces();

    //! Delete a workspace by ID.
    void DeleteWorkspace(const std::string &workspace_id);

    //! Delete rows from a Keboola table using the Storage API delete-rows endpoint.
    //! The WHERE clause parameters are encoded as Storage API query parameters.
    //! Polls the resulting async job to completion.
    //! Returns the number of affected rows, or -1 if the count is unavailable.
    int64_t DeleteRows(const std::string &table_id, const KeboolaDeleteParams &params);

    // -----------------------------------------------------------------------
    // Phase 6: DDL operations
    // -----------------------------------------------------------------------

    //! Create a table in a bucket using the Storage API tables-definition endpoint.
    //! column_defs: vector of {column_name, duckdb_type_string} pairs.
    //! primary_keys: column names to set as PRIMARY KEY in Keboola Storage.
    //! Returns the newly created table's metadata.
    KeboolaTableInfo CreateTable(const std::string &bucket_id,
                                  const std::string &table_name,
                                  const std::vector<std::pair<std::string, std::string>> &column_defs,
                                  const std::vector<std::string> &primary_keys = {});

    //! Drop a table by its full table ID (e.g. "in.c-crm.contacts").
    //! Throws CatalogException if the table does not exist (404).
    void DropTable(const std::string &table_id);

    //! Create a bucket (schema).
    //! stage: "in" or "out"; name: bucket name without stage prefix (e.g. "c-myapp").
    //! Returns the newly created bucket's metadata.
    KeboolaBucketInfo CreateBucket(const std::string &stage, const std::string &name,
                                    const std::string &description = "");

    //! Drop a bucket by its ID.
    //! Throws CatalogException if the bucket does not exist (404).
    void DropBucket(const std::string &bucket_id);

private:
    KeboolaHttpClient http_;

    //! Memoized GetDefaultBackend() result — the backend cannot change within
    //! a client's lifetime, so one token-verify round-trip per client suffices.
    std::string default_backend_cache_;
    bool default_backend_cached_ = false;

    //! Parse column-level metadata from the JSON "columnMetadata" field of a column object.
    //! Returns the best DuckDB type string, using the mapping priority defined in type_mapper.
    std::string MapColumnType(const std::string &col_name,
                              const std::string &metadata_json_for_column);
};

} // namespace duckdb
