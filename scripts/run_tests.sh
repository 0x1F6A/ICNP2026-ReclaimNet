#!/usr/bin/env bash


set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT/build"

"$ROOT/scripts/build.sh" "${1:-Debug}"
ctest --test-dir "$BUILD_DIR" --output-on-failure \
  --parallel "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
