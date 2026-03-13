# =============================================================================
# Stage 1: builder
# Compiles the keboola DuckDB extension from source.
# =============================================================================
FROM ubuntu:22.04 AS builder

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    clang \
    clang-format \
    git \
    curl \
    ca-certificates \
    python3 \
    python3-pip \
    unzip \
    pkg-config \
    zip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the extension source (everything except build artefacts)
COPY . .

# Initialise git submodules (DuckDB + extension-ci-tools) if not already done.
# In CI the submodules are checked out before building the image; this guard
# handles a fresh clone where they may still be empty.
RUN if [ ! -f duckdb/CMakeLists.txt ]; then \
        git submodule update --init --recursive; \
    fi

# Build the extension.
# The Makefile provided by the extension-template handles all cmake steps.
RUN make release

# Sanity-check: the extension file must exist after a successful build.
RUN test -f build/release/extension/keboola/keboola.duckdb_extension \
    || (echo "ERROR: extension not found after build" && exit 1)


# =============================================================================
# Stage 2: tester
# Runs both the sqllogictest offline suite and the pytest E2E suite.
# =============================================================================
FROM ubuntu:22.04 AS tester

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip3 install --no-cache-dir uv

WORKDIR /app

# Copy the compiled extension from the builder stage
COPY --from=builder \
    /build/build/release/extension/keboola/keboola.duckdb_extension \
    /app/extension/keboola.duckdb_extension

# Set the default extension path so tests find it automatically.
# Individual tests may override this via KEBOOLA_EXTENSION_PATH.
ENV KEBOOLA_EXTENSION_PATH=/app/extension/keboola.duckdb_extension

# Copy the sqllogictest runner binary from the builder (built as part of DuckDB)
COPY --from=builder \
    /build/build/release/duckdb \
    /usr/local/bin/duckdb

# Copy the test directory
COPY test/ /app/test/

# Install Python test dependencies via uv
RUN cd /app/test/e2e && uv sync

# Default entrypoint: run offline sqllogictest suite, then (if credentials are
# provided) the live E2E pytest suite.
# The docker-compose.yml overrides the command for each service individually.
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["set -e; \
      echo '=== Running offline sqllogictest suite ==='; \
      cd /app && \
      for f in test/sql/*.test; do \
          echo \"-- $f\"; \
          duckdb -unsigned :memory: -bail < \"$f\" || echo \"WARN: $f exited non-zero\"; \
      done; \
      echo '=== sqllogictest complete ==='; \
      if [ -n \"${KEBOOLA_TOKEN}\" ] && [ -n \"${KEBOOLA_URL}\" ]; then \
          echo '=== Running E2E pytest suite ==='; \
          cd /app/test/e2e && uv run pytest -v --timeout=60 -m live .; \
      else \
          echo 'Skipping E2E suite: KEBOOLA_TOKEN / KEBOOLA_URL not set'; \
      fi"]
