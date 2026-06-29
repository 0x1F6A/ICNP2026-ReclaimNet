# ReclaimNet C++17

ReclaimNet includes the coordinator, node agent, P1/P2/P3 scheduling, application
checkpoint and restore, TLS 1.3 transfers, GPU allocation, and Linux TC data plane.

## Build and test

Dependencies are CMake 3.20 or later, a C++17 compiler, gRPC 1.84 or later,
Protobuf, OpenSSL, SQLite3, yaml-cpp, nlohmann_json, and GoogleTest. Application
runtime integration uses Python, pybind11, PyTorch, and Transformers. The Linux
data plane uses clang, libbpf, and kernel BTF.

```bash
brew install cmake grpc protobuf openssl sqlite yaml-cpp nlohmann-json googletest pybind11
./scripts/build.sh Debug
./scripts/run_tests.sh Debug
```

The pinned Linux gRPC dependency can be built with:

```bash
./scripts/build_grpc.sh /opt/grpc /tmp/reclaimnet-grpc-src
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/opt/grpc
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The Dockerfiles build the dependencies, install the application bridge, and run
the C++ and Python tests. `docker/docker-compose.yaml` starts the coordinator and
agent with separate credentials and the supplied seccomp policy.

## Protocol behavior

- P1 uses capped cube-root bandwidth allocation, checkpoint interval bounds,
  checkpoint payload sizes, and shared-path capacity constraints.
- P2 filters GPU and VRAM capacity, CUDA compatibility, building diversity, and
  hazard estimates, then scores destinations and matches gang ranks.
- P3 admits batches in EDF order and distributes surplus bandwidth with max-min
  water filling along each migration path. Rate updates are versioned and track
  checkpoint sizes and transfer progress.
- Periodic checkpoints are committed after the replica acknowledges them.
  Scheduled reclaim pauses the application, takes a final checkpoint, and
  transfers it within the notice window. Emergency reclaim restores from the
  latest committed checkpoint.
- Recovered jobs restart after all ranks are restored and the source nodes have
  stopped. Pending handoffs, checkpoint metadata, and estimator history are
  stored in SQLite.
- The node agent executes checkpoint plans, controls application lifecycle,
  allocates GPU devices, configures TC flow policies, reports telemetry, and
  exports transfer records.

## Trace format

Trace files use the trace v3 JSON/JSONL format of `trace_all.zip`.

| Field | Unit or meaning |
|---|---|
| `*_bytes` | bytes |
| `*_bps` | bits per second |
| `*_ns`, legacy `timestamp` | integer Unix epoch nanoseconds |
| `*_s`, `tau_seconds` | seconds |
| `lambda_e`, `lambda_i` | events per second |
| Reclaim type | `SCHEDULED=0`, `EMERGENCY=1` |
| Topology level | IntraRack, IntraBuilding, CrossBuilding, CoreCrossing |

Runtime scheduling and telemetry fields are stored in separate SQLite tables.
The trace exporter writes the JSONL files, node inventory, schema, and a
manifest with file sizes, record counts, and SHA-256 hashes.

```bash
python3 scripts/check_trace_compat.py ../trace_all.zip --full
```

The script replays the P1 epochs in the archive through the C++ allocator and
checks reader/writer round trips for the other trace files.

## Run

```bash
./scripts/gen_certs.sh
./build/bin/coordinator configs/coordinator.yaml
./build/bin/node_agent configs/agent.yaml
```

Node identities, addresses, certificate paths, runtime registrations, and
checkpoint directories are configured in the supplied YAML files.
