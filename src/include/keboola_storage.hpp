#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class KeboolaStorageExtension : public StorageExtension {
public:
    KeboolaStorageExtension();
};

} // namespace duckdb
