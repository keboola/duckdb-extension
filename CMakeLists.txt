cmake_minimum_required(VERSION 3.21)

# Extension-specific settings — must be set before the DuckDB cmake include
set(TARGET_NAME keboola)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(src)

# cpp-httplib — use the copy bundled with DuckDB (no vcpkg needed)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/httplib)

# Enable HTTPS support in cpp-httplib (requires OpenSSL, which DuckDB links anyway)
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

set(EXTENSION_SOURCES
    src/keboola_extension.cpp
    src/keboola_secret.cpp
    src/keboola_storage.cpp
    src/keboola_catalog.cpp
    src/keboola_schema.cpp
    src/keboola_table.cpp
    src/keboola_transaction.cpp
    src/keboola_scan.cpp
    src/keboola_insert.cpp
    src/keboola_update.cpp
    src/keboola_delete.cpp
    src/keboola_functions.cpp
    src/http/storage_api_client.cpp
    src/http/query_service_client.cpp
    src/http/importer_client.cpp
    src/util/type_mapper.cpp
    src/util/sql_generator.cpp
    src/util/csv_builder.cpp
)

# Build the extension as both a static and loadable (shared) library.
# build_static_extension() and build_loadable_extension() are provided by the
# DuckDB CMake infrastructure (loaded via extension-ci-tools / the duckdb submodule).
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL for HTTPS support in cpp-httplib
find_package(OpenSSL REQUIRED)
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Installation — required by the DuckDB extension distribution pipeline.
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
