#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class KeboolaStorageExtension : public StorageExtension {
public:
    KeboolaStorageExtension();
};

//! Remove a workspace from the global atexit/signal cleanup registry.
//! Called by OnDetach so we don't double-delete after an explicit DETACH.
void UnregisterWorkspaceFromCleanup(const std::string &workspace_id);

} // namespace duckdb
