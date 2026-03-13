# =============================================================================
# Makefile for the Keboola DuckDB Extension test suite
#
# Targets:
#   test-offline          Run sqllogictest offline suite (no credentials needed)
#   test-e2e              Run pytest E2E live suite (requires KEBOOLA_TOKEN, KEBOOLA_URL)
#   test-all              Run both suites sequentially
#   test-docker-offline   Run offline suite inside Docker
#   test-docker-e2e       Run E2E suite inside Docker (passes host credentials)
#   test-docker-all       Run both Docker-based suites
#   help                  Print this help message
# =============================================================================

# ---------------------------------------------------------------------------
# Configuration — override on the command line as needed
# ---------------------------------------------------------------------------

# Path to the compiled DuckDB CLI (used for sqllogictest)
DUCKDB ?= duckdb

# Default extension path (relative to repo root); override via env var
KEBOOLA_EXTENSION_PATH ?= ../build/release/extension/keboola/keboola.duckdb_extension

# Keboola credentials — must be set in environment for live tests
KEBOOLA_TOKEN ?=
KEBOOLA_URL   ?= https://connection.keboola.com

# Docker Compose project name (avoid collisions in shared CI agents)
COMPOSE_PROJECT_NAME ?= keboola_duckdb_ext_test

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
SQL_TEST_DIR  := $(REPO_ROOT)/test/sql
E2E_TEST_DIR  := $(REPO_ROOT)/test/e2e
DOCKER_DIR    := $(REPO_ROOT)/test/docker

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------

.PHONY: help
help:
	@echo ""
	@echo "Keboola DuckDB Extension — Test Targets"
	@echo "========================================"
	@echo ""
	@echo "  make test-offline          Run sqllogictest offline suite"
	@echo "  make test-e2e              Run pytest E2E live suite"
	@echo "  make test-all              Run both suites sequentially"
	@echo ""
	@echo "  make test-docker-offline   Run offline suite in Docker"
	@echo "  make test-docker-e2e       Run E2E suite in Docker (needs KEBOOLA_TOKEN)"
	@echo "  make test-docker-all       Run both Docker suites"
	@echo ""
	@echo "Environment variables:"
	@echo "  KEBOOLA_TOKEN    Storage API token (required for live tests)"
	@echo "  KEBOOLA_URL      Stack URL (default: https://connection.keboola.com)"
	@echo "  KEBOOLA_EXTENSION_PATH  Path to built .duckdb_extension file"
	@echo ""

# ---------------------------------------------------------------------------
# Offline: sqllogictest suite
# ---------------------------------------------------------------------------

.PHONY: test-offline
test-offline:
	@echo "=== Running offline sqllogictest suite ==="
	@PASS=0; FAIL=0; \
	for f in $(SQL_TEST_DIR)/*.test; do \
		echo "-- $$f"; \
		if $(DUCKDB) -unsigned :memory: -bail < "$$f"; then \
			PASS=$$((PASS+1)); \
		else \
			FAIL=$$((FAIL+1)); \
			echo "FAILED: $$f"; \
		fi; \
	done; \
	echo ""; \
	echo "=== Results: $$PASS passed, $$FAIL failed ==="; \
	exit $$FAIL

# ---------------------------------------------------------------------------
# Live E2E: pytest suite
# ---------------------------------------------------------------------------

.PHONY: test-e2e
test-e2e:
ifndef KEBOOLA_TOKEN
	$(error KEBOOLA_TOKEN is not set. Export it before running: export KEBOOLA_TOKEN=sapi-xxx)
endif
	@echo "=== Running E2E pytest suite ==="
	@cd $(E2E_TEST_DIR) && \
		KEBOOLA_TOKEN="$(KEBOOLA_TOKEN)" \
		KEBOOLA_URL="$(KEBOOLA_URL)" \
		KEBOOLA_EXTENSION_PATH="$(KEBOOLA_EXTENSION_PATH)" \
		uv run pytest -v --timeout=60 -m live .

# ---------------------------------------------------------------------------
# Both suites
# ---------------------------------------------------------------------------

.PHONY: test-all
test-all: test-offline test-e2e

# ---------------------------------------------------------------------------
# Docker: offline suite
# ---------------------------------------------------------------------------

.PHONY: test-docker-offline
test-docker-offline:
	@echo "=== Building Docker image ==="
	@docker compose \
		-f $(DOCKER_DIR)/docker-compose.yml \
		-p $(COMPOSE_PROJECT_NAME) \
		build
	@echo "=== Running offline tests in Docker ==="
	@docker compose \
		-f $(DOCKER_DIR)/docker-compose.yml \
		-p $(COMPOSE_PROJECT_NAME) \
		run --rm test-offline

# ---------------------------------------------------------------------------
# Docker: E2E suite (passes host credentials into container)
# ---------------------------------------------------------------------------

.PHONY: test-docker-e2e
test-docker-e2e:
ifndef KEBOOLA_TOKEN
	$(error KEBOOLA_TOKEN is not set. Export it: export KEBOOLA_TOKEN=sapi-xxx)
endif
	@echo "=== Building Docker image ==="
	@docker compose \
		-f $(DOCKER_DIR)/docker-compose.yml \
		-p $(COMPOSE_PROJECT_NAME) \
		build
	@echo "=== Running E2E tests in Docker ==="
	@KEBOOLA_TOKEN="$(KEBOOLA_TOKEN)" \
	KEBOOLA_URL="$(KEBOOLA_URL)" \
	docker compose \
		-f $(DOCKER_DIR)/docker-compose.yml \
		-p $(COMPOSE_PROJECT_NAME) \
		run --rm test-e2e

# ---------------------------------------------------------------------------
# Docker: both suites
# ---------------------------------------------------------------------------

.PHONY: test-docker-all
test-docker-all: test-docker-offline test-docker-e2e
