#!/usr/bin/env bash


set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT/build"

if [ ! -d "$BUILD_DIR" ]; then
  "$ROOT/scripts/build.sh" Debug
fi

cd "$BUILD_DIR"
ctest --output-on-failure --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
