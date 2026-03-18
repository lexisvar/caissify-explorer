# ---- Chef stage: install cargo-chef ----
FROM rust:1.85 AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Install RocksDB build dependencies (io-uring requires liburing-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    liburing-dev \
    && rm -rf /var/lib/apt/lists/*

# ---- Planner stage: compute the dependency recipe ----
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Build stage ----
FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json

# Pre-build all dependencies (this layer is cached as long as Cargo.toml/Cargo.lock don't change)
RUN cargo chef cook --release --recipe-path recipe.json

# Copy actual sources and do the final build
COPY . .
RUN cargo build --release --bin caissify-explorer
RUN cd import-pgn && cargo build --release

# ---- Runtime stage ----
# Must match the Debian version used by rust:1.85 (bookworm) to avoid GLIBC mismatches.
FROM debian:bookworm-slim

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
