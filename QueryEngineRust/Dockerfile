# QueryEngine Rust/Dockerfile
# Multi-stage build for Rust application

FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="QueryEngine Rust service for SketchDB"

WORKDIR /code

# Copy the CommonDependencies directory
COPY CommonDependencies ./CommonDependencies

# Copy manifests first for better layer caching
COPY QueryEngineRust/Cargo.toml QueryEngineRust/Cargo.lock ./QueryEngineRust/

# Create a dummy main.rs to build dependencies
RUN mkdir -p QueryEngineRust/src && echo "fn main() {}" > QueryEngineRust/src/main.rs

# Build dependencies (this layer will be cached)
RUN cd QueryEngineRust && cargo build --release && rm -rf src/

# Copy source code
COPY QueryEngineRust/src ./QueryEngineRust/src

# Build the actual application
RUN cd QueryEngineRust && touch src/main.rs && cargo build --release

# Runtime stage with Ubuntu 24.04 (has newer glibc/libstdc++)
FROM ubuntu:24.04

WORKDIR /app

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /code/QueryEngineRust/target/release/query_engine_rust /usr/local/bin/query_engine_rust

# Create a non-root user
RUN useradd -r -s /bin/false queryengine

# Expose the HTTP server port
EXPOSE 8088

# Switch to non-root user
USER queryengine

# Use ENTRYPOINT to allow passing command line arguments
ENTRYPOINT ["query_engine_rust"]
