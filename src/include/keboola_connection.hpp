#pragma once

#include "http/storage_api_client.hpp"

#include <memory>
#include <string>

namespace duckdb {

//! Connection state shared across all catalog objects for one ATTACH.
//! Owned by KeboolaCatalog and referenced via shared_ptr everywhere else.
struct KeboolaConnection {
    std::string token;
    std::string url;
    std::string branch;
    std::string branch_id;
    std::string workspace_id;
    KeboolaServiceUrls service_urls;
    std::shared_ptr<StorageApiClient> storage_client;
    //! When true, tables are lazily pulled into local storage on first scan.
    bool snapshot_mode = false;
};

} // namespace duckdb
