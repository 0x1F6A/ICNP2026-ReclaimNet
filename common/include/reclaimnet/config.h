

#pragma once

#include "reclaimnet/result.h"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace reclaimnet {

struct CoordinatorConfig {

  std::string listen_address = "0.0.0.0:50051";

  double epoch_seconds = 30.0;

  uint64_t B_ckpt_bps = 7'000'000'000ULL;

  double migration_share = 0.70;
  double research_min_bps = 50'000'000.0;
  double checkpoint_beta = 0.20;
  double checkpoint_loss_budget_s = 18'000.0;

  double alpha = 1.0;
  double beta = 2.0;
  double gamma = 0.5;

  std::string sqlite_path = "reclaimnet.db";

  uint16_t prometheus_port = 9090;

  std::string tls_cert_path = "certs/coordinator.crt";
  std::string tls_key_path = "certs/coordinator.key";
  std::string tls_ca_path = "certs/ca.crt";
  bool mtls_enabled = true;

  double heartbeat_timeout_s = 30.0;

  std::string topology_file = "configs/topology.json";

  double hazard_decay_s = 300.0;

  uint32_t max_migration_queue = 256;
  double hazard_window_s = 3600.0;
  double capacity_window_s = 300.0;
  double capacity_confidence_factor = 0.8;
  size_t p2_k_min = 2;
  double p2_load_limit_pct = 80.0;
  double minimum_useful_transfer_s = 1.0;
  double restore_time_s = 5.0;
  std::string trace_dir;
  std::string run_id = "reclaimnet-runtime";
  std::string jwt_private_key_path;
  std::string jwt_public_key_path;
  int64_t jwt_lifetime_s = 120;
  bool jwt_enabled = false;
};

struct AgentConfig {

  std::string node_id = "";
  std::string building = "";
  std::string switch_id = "";

  std::string coordinator_address = "localhost:50051";

  std::string listen_address = "0.0.0.0:50052";

  uint64_t total_vram_bytes = 0;
  uint64_t link_capacity_bps = 10'000'000'000ULL;

  std::string iface = "eth0";

  std::string checkpoint_dir = "/var/reclaimnet/checkpoints";

  std::string tls_cert_path = "certs/agent.crt";
  std::string tls_key_path = "certs/agent.key";
  std::string tls_ca_path = "certs/ca.crt";
  bool mtls_enabled = true;

  double telemetry_interval_s = 10.0;

  double heartbeat_interval_s = 5.0;

  uint32_t migration_recv_buf = 4 * 1024 * 1024;
  std::string advertise_address;
  uint16_t migration_port = 7600;
  double control_poll_s = 0.1;
  std::string bpf_object_dir = "data_plane";
  std::string jobs_file;
  std::string jwt_public_key_path;
  bool ebpf_enabled = false;
  bool gpu_isolation_enabled = false;
  std::string seccomp_profile = "docker/seccomp.json";
};

Result<CoordinatorConfig> load_coordinator_config(const std::string &path);

Result<AgentConfig> load_agent_config(const std::string &path);

VoidResult validate_coordinator_config(const CoordinatorConfig &cfg);

VoidResult validate_agent_config(const AgentConfig &cfg);

}
