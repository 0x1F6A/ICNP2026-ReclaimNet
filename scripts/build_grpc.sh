#!/usr/bin/env bash
set -euo pipefail
prefix="${1:-/opt/grpc}"
source_dir="${2:-/tmp/reclaimnet-grpc-1.84.0}"
git clone --depth 1 --branch v1.84.0 --recurse-submodules --shallow-submodules https://github.com/grpc/grpc.git "$source_dir"
test "$(git -C "$source_dir" rev-parse HEAD)" = 3252a89f10d8e92997862167ca7d095ecda85973
cmake -S "$source_dir" -B "$source_dir/build" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$prefix" \
  -DBUILD_SHARED_LIBS=ON -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF \
  -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package \
  -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
  -Dprotobuf_BUILD_TESTS=OFF -DABSL_PROPAGATE_CXX_STD=ON
cmake --build "$source_dir/build" --parallel "${RECLAIMNET_BUILD_JOBS:-4}"
cmake --install "$source_dir/build"
printf '%s\n' "$prefix/lib" > /etc/ld.so.conf.d/reclaimnet-grpc.conf
ldconfig
