#!/usr/bin/env bash
set -euo pipefail
cd /opt/reclaimnet
bpf_tool=bpftool
for tool in /usr/lib/linux-tools-*/bpftool; do
  if [ -x "$tool" ]; then bpf_tool="$tool"; break; fi
done
make -C data_plane BPFTOOL="$bpf_tool" -j "${RECLAIMNET_BUILD_JOBS:-3}"
exec bin/node_agent "${1:-/etc/reclaimnet/agent.yaml}"
