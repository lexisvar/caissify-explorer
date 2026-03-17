# ---- Build stage ----
FROM rust:latest AS builder

WORKDIR /app

# Install RocksDB build dependencies (io-uring requires liburing-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    liburing-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first (better layer caching)
COPY Cargo.toml Cargo.lock ./
COPY import-pgn/Cargo.toml ./import-pgn/Cargo.toml

# Create dummy sources so Cargo can resolve dependencies
RUN mkdir -p src import-pgn/src/bin benches && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    echo "fn main() {}" > import-pgn/src/bin/import-lichess.rs && \
    echo "fn main() {}" > import-pgn/src/bin/import-caissify.rs && \
    echo "fn main() {}" > benches/benches.rs

RUN cargo build --release --bin caissify-explorer 2>/dev/null || true
RUN cd import-pgn && cargo build --release 2>/dev/null || true

# Copy actual sources and rebuild
COPY src ./src
COPY import-pgn/src ./import-pgn/src
COPY benches ./benches

# Touch to force rebuild
RUN touch src/main.rs src/lib.rs import-pgn/src/bin/import-caissify.rs import-pgn/src/bin/import-lichess.rs

RUN cargo build --release --bin caissify-explorer
RUN cd import-pgn && cargo build --release

# ---- Runtime stage ----
# Must match the Debian version used by rust:latest to avoid GLIBC mismatches.
FROM debian:trixie-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    liburing2 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/caissify-explorer /usr/local/bin/caissify-explorer
COPY --from=builder /app/import-pgn/target/release/import-caissify /usr/local/bin/import-caissify

# Data directory for RocksDB
RUN mkdir -p /data

EXPOSE 9002

ENV JEMALLOC_SYS_WITH_MALLOC_CONF="abort_conf:true,background_thread:true,metadata_thp:auto,dirty_decay_ms:30000,muzzy_decay_ms:30000"
ENV EXPLORER_LOG="info"

ENTRYPOINT ["caissify-explorer"]
CMD ["--bind", "0.0.0.0:9002", "--db", "/data"]
