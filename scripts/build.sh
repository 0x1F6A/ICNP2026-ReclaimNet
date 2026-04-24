#!/usr/bin/env bash



set -euo pipefail
BUILD_TYPE="${1:-Release}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT/build"

echo "==> Configuring (${BUILD_TYPE})"
cmake -S "$ROOT" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

echo "==> Building"
cmake --build "$BUILD_DIR" --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

echo "==> Done: $BUILD_DIR"
