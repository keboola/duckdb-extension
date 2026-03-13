PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Extension metadata — also declared in extension_config.cmake
EXT_NAME   := keboola
EXT_CONFIG := $(PROJ_DIR)extension_config.cmake

# Delegate all standard build targets (release, debug, test, clean, …) to the
# shared extension-ci-tools Makefile that understands the DuckDB build system.
include $(PROJ_DIR)extension-ci-tools/makefiles/duckdb_extension.Makefile
