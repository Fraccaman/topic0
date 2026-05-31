# syntax=docker/dockerfile:1

# ---- chef base ----
# Latest stable Rust; the in-repo rust-toolchain.toml pins the exact channel.
FROM rust:1-slim-bookworm AS chef
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

# ---- planner: capture dependency graph ----
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- builder ----
FROM chef AS builder
# Build & cache deps only (recipe changes only when Cargo.toml/lock change).
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# Now the source; only this layer rebuilds on code change.
COPY . .
RUN cargo build --release --bin indexer --bin indexer-query \
    && mkdir -p /out \
    && cp target/release/indexer target/release/indexer-query /out/

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home indexer

COPY --from=builder /out/indexer /usr/local/bin/indexer
COPY --from=builder /out/indexer-query /usr/local/bin/indexer-query

# ABIs are read at runtime by config; bundle them in the image.
COPY abis ./abis

USER indexer

# Subcommands (overridden per-service in compose):
#   migrate | backfill | resync | follow | run | decode
ENTRYPOINT ["indexer"]
