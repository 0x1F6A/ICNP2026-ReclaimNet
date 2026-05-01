

#include "reclaimnet/config.h"
#include "reclaimnet/log.h"

#include <cmath>
#include <fstream>
#include <sstream>
#include <stdexcept>

#if RECLAIMNET_HAVE_YAML
#include <yaml-cpp/yaml.h>
#define HAVE_YAML_CPP 1
#else
#define HAVE_YAML_CPP 0
#endif

namespace reclaimnet {

#if HAVE_YAML_CPP

template <typename T>
static void yaml_get(const YAML::Node &node, const char *key, T &out) {
  if (node[key])
    out = node[key].as<T>();
}

static Result<CoordinatorConfig> parse_coordinator(const YAML::Node &root) {
  CoordinatorConfig cfg;
  yaml_get(root, "listen_address", cfg.listen_address);
  yaml_get(root, "epoch_seconds", cfg.epoch_seconds);
  yaml_get(root, "B_ckpt_bps", cfg.B_ckpt_bps);
  yaml_get(root, "migration_share", cfg.migration_share);
  if (root["p3_beta"])
    cfg.migration_share = 1.0 - root["p3_beta"].as<double>();
  yaml_get(root, "p3_bmin_bps", cfg.research_min_bps);
  yaml_get(root, "p1_beta", cfg.checkpoint_beta);
  yaml_get(root, "p1_loss_budget_s", cfg.checkpoint_loss_budget_s);
  yaml_get(root, "alpha", cfg.alpha);
  yaml_get(root, "beta", cfg.beta);
  yaml_get(root, "gamma", cfg.gamma);
  yaml_get(root, "sqlite_path", cfg.sqlite_path);
  yaml_get(root, "prometheus_port", cfg.prometheus_port);
  yaml_get(root, "tls_cert_path", cfg.tls_cert_path);
  yaml_get(root, "tls_key_path", cfg.tls_key_path);
  yaml_get(root, "tls_ca_path", cfg.tls_ca_path);
  yaml_get(root, "mtls_enabled", cfg.mtls_enabled);
  yaml_get(root, "heartbeat_timeout_s", cfg.heartbeat_timeout_s);
  yaml_get(root, "topology_file", cfg.topology_file);
  yaml_get(root, "hazard_decay_s", cfg.hazard_decay_s);
  yaml_get(root, "max_migration_queue", cfg.max_migration_queue);
  yaml_get(root, "hazard_window_s", cfg.hazard_window_s);
  yaml_get(root, "capacity_window_s", cfg.capacity_window_s);
  yaml_get(root, "capacity_confidence_factor", cfg.capacity_confidence_factor);
  yaml_get(root, "p2_k_min", cfg.p2_k_min);
  yaml_get(root, "p2_load_limit_pct", cfg.p2_load_limit_pct);
  yaml_get(root, "p3_min_useful_transfer_s", cfg.minimum_useful_transfer_s);
  yaml_get(root, "restore_time_s", cfg.restore_time_s);
  yaml_get(root, "trace_dir", cfg.trace_dir);
  yaml_get(root, "run_id", cfg.run_id);
  yaml_get(root, "jwt_private_key_path", cfg.jwt_private_key_path);
  yaml_get(root, "jwt_public_key_path", cfg.jwt_public_key_path);
  yaml_get(root, "jwt_lifetime_s", cfg.jwt_lifetime_s);
  yaml_get(root, "jwt_enabled", cfg.jwt_enabled);
  return cfg;
}

static Result<AgentConfig> parse_agent(const YAML::Node &root) {
  AgentConfig cfg;
  yaml_get(root, "node_id", cfg.node_id);
  yaml_get(root, "building", cfg.building);
  yaml_get(root, "switch_id", cfg.switch_id);
  yaml_get(root, "coordinator_address", cfg.coordinator_address);
  yaml_get(root, "listen_address", cfg.listen_address);
  yaml_get(root, "total_vram_bytes", cfg.total_vram_bytes);
  yaml_get(root, "link_capacity_bps", cfg.link_capacity_bps);
  yaml_get(root, "iface", cfg.iface);
  yaml_get(root, "checkpoint_dir", cfg.checkpoint_dir);
  yaml_get(root, "tls_cert_path", cfg.tls_cert_path);
  yaml_get(root, "tls_key_path", cfg.tls_key_path);
  yaml_get(root, "tls_ca_path", cfg.tls_ca_path);
  yaml_get(root, "mtls_enabled", cfg.mtls_enabled);
  yaml_get(root, "telemetry_interval_s", cfg.telemetry_interval_s);
  yaml_get(root, "heartbeat_interval_s", cfg.heartbeat_interval_s);
  yaml_get(root, "migration_recv_buf", cfg.migration_recv_buf);
  yaml_get(root, "advertise_address", cfg.advertise_address);
  yaml_get(root, "migration_port", cfg.migration_port);
  yaml_get(root, "control_poll_s", cfg.control_poll_s);
  yaml_get(root, "bpf_object_dir", cfg.bpf_object_dir);
  yaml_get(root, "jobs_file", cfg.jobs_file);
  yaml_get(root, "jwt_public_key_path", cfg.jwt_public_key_path);
  yaml_get(root, "ebpf_enabled", cfg.ebpf_enabled);
  yaml_get(root, "gpu_isolation_enabled", cfg.gpu_isolation_enabled);
  yaml_get(root, "seccomp_profile", cfg.seccomp_profile);
  return cfg;
}

#endif

Result<CoordinatorConfig> load_coordinator_config(const std::string &path) {
#if HAVE_YAML_CPP
  try {
    YAML::Node root = YAML::LoadFile(path);
    return parse_coordinator(root);
  } catch (const YAML::Exception &e) {
    return Error{"YAML parse error in " + path + ": " + e.what()};
  } catch (const std::exception &e) {
    return Error{"Failed to load coordinator config from " + path + ": " +
                 e.what()};
  }
#else
  return Error{"YAML support is required to load " + path};
#endif
}

Result<AgentConfig> load_agent_config(const std::string &path) {
#if HAVE_YAML_CPP
  try {
    YAML::Node root = YAML::LoadFile(path);
    return parse_agent(root);
  } catch (const YAML::Exception &e) {
    return Error{"YAML parse error in " + path + ": " + e.what()};
  } catch (const std::exception &e) {
    return Error{"Failed to load agent config from " + path + ": " + e.what()};
  }
#else
  return Error{"YAML support is required to load " + path};
#endif
}

VoidResult validate_coordinator_config(const CoordinatorConfig &cfg) {
  if (cfg.listen_address.empty())
    return err_void("listen_address must not be empty");
  if (!std::isfinite(cfg.epoch_seconds) || cfg.epoch_seconds <= 0.0)
    return err_void("epoch_seconds must be positive");
  if (cfg.B_ckpt_bps == 0)
    return err_void("B_ckpt_bps must be positive");
  if (!std::isfinite(cfg.migration_share) || cfg.migration_share <= 0.0 ||
      cfg.migration_share > 1.0)
    return err_void("migration_share must be in (0,1]");
  if (!std::isfinite(cfg.heartbeat_timeout_s) || cfg.heartbeat_timeout_s <= 0.0)
    return err_void("heartbeat_timeout_s must be positive");
  if (!std::isfinite(cfg.checkpoint_beta) || cfg.checkpoint_beta <= 0 ||
      cfg.checkpoint_beta > 1)
    return err_void("p1_beta must be in (0,1]");
  if (!std::isfinite(cfg.checkpoint_loss_budget_s) ||
      cfg.checkpoint_loss_budget_s <= 0)
    return err_void("p1_loss_budget_s must be positive");
  if (!std::isfinite(cfg.research_min_bps) || cfg.research_min_bps < 0)
    return err_void("p3_bmin_bps must be nonnegative");
  if (!std::isfinite(cfg.hazard_decay_s) || cfg.hazard_decay_s <= 0)
    return err_void("hazard_decay_s must be positive");
  return ok_void();
}

VoidResult validate_agent_config(const AgentConfig &cfg) {
  if (cfg.coordinator_address.empty())
    return err_void("coordinator_address must not be empty");
  if (cfg.listen_address.empty())
    return err_void("listen_address must not be empty");
  if (cfg.iface.empty())
    return err_void("iface must not be empty");
  if (cfg.heartbeat_interval_s <= 0.0)
    return err_void("heartbeat_interval_s must be positive");
  return ok_void();
}

}
