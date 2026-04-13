# Multi-stage build for network-control-server
# Build context must be the network-control-server directory.
# The asap_sketchlib crate is copied in via docker-build.sh.

# --- Stage 1: Build ---
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the vendored sketchlib dependency (path must match patched Cargo.toml)
COPY .docker-deps/asap_sketchlib /build/.docker-deps/asap_sketchlib

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs so cargo can resolve deps and cache them
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Copy actual source
COPY src/ src/

# Touch main.rs so cargo detects source change
RUN touch src/main.rs

RUN cargo build --release

# --- Stage 2: Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/network-control-server /app/network-control-server

# Copy default config files
COPY server-config.yaml /app/server-config.yaml

EXPOSE 10101

ENV NCS_CONFIG_PATH=/app/server-config.yaml
ENV NCS_SERVER_HOST=0.0.0.0

ENTRYPOINT ["/app/network-control-server"]
