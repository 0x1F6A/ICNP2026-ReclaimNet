/// @file config.h
/// @brief YAML-backed configuration loader for coordinator and agent processes.
///
/// Structs are populated from YAML files at startup; defaults are provided for
/// all optional fields.  mTLS paths, Prometheus port, and SQLite persistence
/// path are required for the coordinator.

#pragma once

#include <string>
#include <cstdint>
#include <vector>
#include <optional>
#include "reclaimnet/result.h"

namespace reclaimnet {

// ── Coordinator configuration ─────────────────────────────────────────────────

/// Configuration for the ReclaimNet coordinator process.
struct CoordinatorConfig {
  // gRPC server
  std::string listen_address = "0.0.0.0:50051";

  // Scheduling epoch (P1 §V)
  double epoch_seconds = 5.0;

  // Total checkpoint bandwidth budget (bits/s) shared across all jobs (P1 §V).
  uint64_t B_ckpt_bps = 1'000'000'000ULL;  // 1 Gbps default

  // Migration share of link capacity for P3 §VII.
  double migration_share = 0.40;

  // P2 composite-cost weights §VI: cost = α·T + β·(1-S) + γ·D.
  double alpha = 1.0;  ///< Transfer-time weight.
  double beta  = 2.0;  ///< Unreliability weight.
  double gamma = 0.5;  ///< Topology-distance weight.

  // Persistence (SQLite).
  std::string sqlite_path = "reclaimnet.db";

  // Prometheus exposition.
  uint16_t prometheus_port = 9090;

  // mTLS certificates.
  std::string tls_cert_path = "certs/coordinator.crt";
  std::string tls_key_path  = "certs/coordinator.key";
  std::string tls_ca_path   = "certs/ca.crt";
  bool        mtls_enabled  = true;

  // Heartbeat timeout (seconds): node declared OFFLINE after this.
  double heartbeat_timeout_s = 30.0;

  // Topology description file (YAML).
  std::string topology_file = "configs/topology.yaml";

  // Hazard estimator decay constant (seconds) – EWMA α = exp(-1/tau_decay).
  double hazard_decay_s = 300.0;

  // Maximum queue depth for pending migration requests.
  uint32_t max_migration_queue = 256;
};

// ── Agent configuration ───────────────────────────────────────────────────────

/// Configuration for a ReclaimNet node agent process.
struct AgentConfig {
  // Identity.
  std::string node_id   = "";   ///< Set from hostname if empty.
  std::string building  = "";
  std::string switch_id = "";

  // Coordinator endpoint.
  std::string coordinator_address = "localhost:50051";

  // gRPC server (for coordinator → agent RPCs).
  std::string listen_address = "0.0.0.0:50052";

  // Hardware capacities.
  uint64_t total_vram_bytes  = 0;   ///< Queried from nvidia-smi if 0.
  uint64_t link_capacity_bps = 10'000'000'000ULL;  // 10 Gbps default

  // Network interface for eBPF attachment.
  std::string iface = "eth0";

  // Checkpoint storage directory.
  std::string checkpoint_dir = "/var/reclaimnet/checkpoints";

  // mTLS.
  std::string tls_cert_path = "certs/agent.crt";
  std::string tls_key_path  = "certs/agent.key";
  std::string tls_ca_path   = "certs/ca.crt";
  bool        mtls_enabled  = true;

  // Telemetry reporting interval (seconds).
  double telemetry_interval_s = 10.0;

  // Heartbeat interval (seconds).
  double heartbeat_interval_s = 5.0;

  // Migration client TCP receive-buffer size (bytes).
  uint32_t migration_recv_buf = 4 * 1024 * 1024;  // 4 MiB
};

// ── Loader functions ──────────────────────────────────────────────────────────

/// Load coordinator config from a YAML file.
/// Returns Error if the file cannot be opened or a required field is missing.
Result<CoordinatorConfig> load_coordinator_config(const std::string& path);

/// Load agent config from a YAML file.
Result<AgentConfig> load_agent_config(const std::string& path);

/// Validate a coordinator config; returns Error describing the first violation.
VoidResult validate_coordinator_config(const CoordinatorConfig& cfg);

/// Validate an agent config.
VoidResult validate_agent_config(const AgentConfig& cfg);

}  // namespace reclaimnet
