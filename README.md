# ReclaimNet

**ReclaimNet** is a production-grade C++17 research prototype for voluntary GPU sharing, targeting ICNP 2026. It implements three cooperative protocols:

| Protocol | Name | Core Idea |
|----------|------|-----------|
| P1 | Reclaim-Aware Checkpoint Scheduling | Optimal checkpoint interval Δt* = √(2C / λ_e B_eff) |
| P2 | Volatility-Aware Destination Selection | composite\_cost = α·T + β·(1−S) + γ·D |
| P3 | Migration Traffic Control | 40% link share + per-flow TC BPF token bucket |

## Architecture

```
coordinator/          ← gRPC server, epoch loop, P1/P2/P3 logic, SQLite
node_agent/           ← heartbeat, checkpoint, migration sender/receiver
data_plane/           ← eBPF TC ratelimit + XDP kill-switch (Linux only)
proto/                ← Protobuf + gRPC service definitions
common/               ← Result<T>, Logger, Config, mTLS, Prometheus
```

## Building

### Prerequisites

- CMake ≥ 3.20, C++17 compiler
- Optional (full feature set): gRPC, Protobuf, SQLite3, yaml-cpp, nlohmann/json
- Linux only: libbpf, clang (for eBPF programs)

```bash
# Install dependencies (Ubuntu 22.04)
sudo apt-get install -y build-essential cmake \
  libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
  libsqlite3-dev libyaml-cpp-dev nlohmann-json3-dev

# Build
./scripts/build.sh Release

# Run tests
./scripts/run_tests.sh
```

### macOS

The project builds on macOS (Intel/Apple Silicon) with gRPC installed via Homebrew. eBPF programs are silently disabled.

```bash
brew install grpc protobuf sqlite yaml-cpp nlohmann-json
./scripts/build.sh Release
```

## Running

```bash
# Generate development certificates
./scripts/gen_certs.sh

# Start coordinator
./build/coordinator configs/coordinator.yaml

# Start agent (in another terminal)
./build/node_agent configs/agent.yaml
```

## Docker

```bash
cd docker
docker compose up --build
```

## P1: Checkpoint Scheduling

The optimal checkpoint interval for job *i* is:

```
Δt*_i = sqrt(2 · C_i / (λ_e · B_eff_i))
```

Bandwidth allocation uses cube-root proportional sharing with iterative water-filling to respect per-job write caps:

```
b_i = B_ckpt · (λ_i · C_i)^(1/3) / Σ_j (λ_j · C_j)^(1/3)
```

## P2: Destination Selection

Infeasibility check: `C/B + T_r > τ` → skip candidate.

Composite cost: `α · T_transfer + β · (1 − exp(−λ_d · H)) + γ · dist(level)`

## P3: Traffic Control

- Reserves `migration_share × link_capacity` for migration traffic.
- eBPF TC program enforces per-flow token-bucket rate limits.
- XDP kill-switch drops all incoming packets during emergency reclaim.

## License

MIT – see [LICENSE](LICENSE).
