






#pragma once

#include <string>
#include <cstdint>
#include <vector>
#include <optional>
#include "reclaimnet/result.h"

namespace reclaimnet {




struct CoordinatorConfig {

  std::string listen_address = "0.0.0.0:50051";


  double epoch_seconds = 5.0;


  uint64_t B_ckpt_bps = 1'000'000'000ULL;


  double migration_share = 0.40;


  double alpha = 1.0;
  double beta  = 2.0;
  double gamma = 0.5;


  std::string sqlite_path = "reclaimnet.db";


  uint16_t prometheus_port = 9090;


  std::string tls_cert_path = "certs/coordinator.crt";
  std::string tls_key_path  = "certs/coordinator.key";
  std::string tls_ca_path   = "certs/ca.crt";
  bool        mtls_enabled  = true;


  double heartbeat_timeout_s = 30.0;


  std::string topology_file = "configs/topology.yaml";


  double hazard_decay_s = 300.0;


  uint32_t max_migration_queue = 256;
};




struct AgentConfig {

  std::string node_id   = "";
  std::string building  = "";
  std::string switch_id = "";


  std::string coordinator_address = "localhost:50051";


  std::string listen_address = "0.0.0.0:50052";


  uint64_t total_vram_bytes  = 0;
  uint64_t link_capacity_bps = 10'000'000'000ULL;


  std::string iface = "eth0";


  std::string checkpoint_dir = "/var/reclaimnet/checkpoints";


  std::string tls_cert_path = "certs/agent.crt";
  std::string tls_key_path  = "certs/agent.key";
  std::string tls_ca_path   = "certs/ca.crt";
  bool        mtls_enabled  = true;


  double telemetry_interval_s = 10.0;


  double heartbeat_interval_s = 5.0;


  uint32_t migration_recv_buf = 4 * 1024 * 1024;
};





Result<CoordinatorConfig> load_coordinator_config(const std::string& path);


Result<AgentConfig> load_agent_config(const std::string& path);


VoidResult validate_coordinator_config(const CoordinatorConfig& cfg);


VoidResult validate_agent_config(const AgentConfig& cfg);

}
