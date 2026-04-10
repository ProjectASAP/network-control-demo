#!/usr/bin/env bash
# docker-build.sh — build the network-control-server Docker image
#
# Usage:
#   ./docker-build.sh [docker build args...]
#
# Example:
#   ./docker-build.sh -t network-control-server:latest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKETCHLIB_SRC="$SCRIPT_DIR/../../../asap_sketchlib"
DOCKER_DEPS_DIR="$SCRIPT_DIR/.docker-deps"

# --- Prepare vendored dependencies ---
echo "copying asap_sketchlib into build context..."
rm -rf "$DOCKER_DEPS_DIR/asap_sketchlib"
mkdir -p "$DOCKER_DEPS_DIR/asap_sketchlib"
# Copy only what cargo needs (exclude target dir)
rsync -a --exclude='target' "$SKETCHLIB_SRC/" "$DOCKER_DEPS_DIR/asap_sketchlib/"

# --- Patch Cargo.toml to use the vendored path ---
echo "patching Cargo.toml for Docker build..."
cp "$SCRIPT_DIR/Cargo.toml" "$SCRIPT_DIR/Cargo.toml.bak"

sed -i 's|path = "../../../asap_sketchlib"|path = ".docker-deps/asap_sketchlib"|' \
    "$SCRIPT_DIR/Cargo.toml"

cleanup() {
    echo "restoring original Cargo.toml..."
    mv "$SCRIPT_DIR/Cargo.toml.bak" "$SCRIPT_DIR/Cargo.toml"
}
trap cleanup EXIT

# --- Build Docker image ---
echo "building Docker image..."
docker build "$@" "$SCRIPT_DIR"

echo "done."
