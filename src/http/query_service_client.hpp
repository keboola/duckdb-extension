#pragma once

#include "http/http_client.hpp"

#include <string>
#include <vector>

namespace duckdb {

//! Metadata for a single column returned by the Query Service.
struct QueryServiceColumn {
    std::string name;
    std::string type; //!< Type string as returned by the Query Service (e.g. "TEXT", "NUMBER")
};

//! Result of a Query Service query execution.
struct QueryServiceResult {
    std::vector<QueryServiceColumn> columns;
    //! All rows as string values (nulls are represented as empty strings with null_mask)
    std::vector<std::vector<std::string>> rows;
    //! null_mask[row][col] == true means the value is NULL
    std::vector<std::vector<bool>> null_mask;
    bool has_more = false;
    int64_t total_rows = -1;
};

//! Client for the Keboola Query Service (workspace query API).
//! Submits SQL, polls until completion, and fetches paginated results.
class QueryServiceClient {
public:
    QueryServiceClient(const std::string &query_service_url,
                       const std::string &storage_token,
                       const std::string &branch_id,
                       const std::string &workspace_id);

    //! Submit SQL, poll until done, fetch all result pages.
    //! Returns all rows with string values. Caller converts to DuckDB types.
    QueryServiceResult ExecuteQuery(const std::string &sql);

    //! POST the query and return the job ID.
    std::string SubmitQuery(const std::string &sql);

    //! Poll GET /api/v1/queries/{job_id} with exponential backoff until status != "processing".
    //! Throws IOException on timeout or error status. Returns the statement ID.
    std::string PollUntilDone(const std::string &job_id);

    //! Fetch a single page of results for a completed job.
    //! offset: row offset for pagination. Returns the page result.
    QueryServiceResult FetchResultPage(const std::string &job_id,
                                       const std::string &statement_id,
                                       int64_t offset);

private:
    //! Parse columns, numberOfRows, and row data from a single JSON result page
    //! into `result`. Appends rows/null_mask (does not clear them first).
    //! Returns the number of rows parsed from this page.
    static int64_t ParseResultPageJson(yyjson_val *root, QueryServiceResult &result,
                                       bool parse_columns);

    //! Fetch all result pages for a completed job.
    QueryServiceResult FetchResults(const std::string &job_id, const std::string &statement_id);

    KeboolaHttpClient http_;
    std::string branch_id_;
    std::string workspace_id_;

    static constexpr int QUERY_PAGE_SIZE = 10000;
    static constexpr int POLL_INITIAL_MS = 100;
    static constexpr double POLL_BACKOFF = 1.5;
    static constexpr int POLL_MAX_MS = 2000;
    static constexpr int POLL_TIMEOUT_MS = 120000;
};

} // namespace duckdb
