# This file is included by DuckDB's build system to discover the keboola extension.

duckdb_extension_load(keboola
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
