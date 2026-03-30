




#include "reclaimnet/config.h"
#include "reclaimnet/log.h"

#include <fstream>
#include <sstream>
#include <stdexcept>


#if __has_include(<yaml-cpp/yaml.h>)
#  include <yaml-cpp/yaml.h>
#  define HAVE_YAML_CPP 1
#else
#  define HAVE_YAML_CPP 0
#endif

namespace reclaimnet {



#if HAVE_YAML_CPP

template <typename T>
static void yaml_get(const YAML::Node& node, const char* key, T& out) {
  if (node[key]) out = node[key].as<T>();
}

static Result<CoordinatorConfig> parse_coordinator(const YAML::Node& root) {
  CoordinatorConfig cfg;
  yaml_get(root, "listen_address",      cfg.listen_address);
  yaml_get(root, "epoch_seconds",       cfg.epoch_seconds);
  yaml_get(root, "B_ckpt_bps",          cfg.B_ckpt_bps);
  yaml_get(root, "migration_share",     cfg.migration_share);
  yaml_get(root, "alpha",               cfg.alpha);
  yaml_get(root, "beta",                cfg.beta);
  yaml_get(root, "gamma",               cfg.gamma);
  yaml_get(root, "sqlite_path",         cfg.sqlite_path);
  yaml_get(root, "prometheus_port",     cfg.prometheus_port);
  yaml_get(root, "tls_cert_path",       cfg.tls_cert_path);
  yaml_get(root, "tls_key_path",        cfg.tls_key_path);
  yaml_get(root, "tls_ca_path",         cfg.tls_ca_path);
  yaml_get(root, "mtls_enabled",        cfg.mtls_enabled);
  yaml_get(root, "heartbeat_timeout_s", cfg.heartbeat_timeout_s);
  yaml_get(root, "topology_file",       cfg.topology_file);
  yaml_get(root, "hazard_decay_s",      cfg.hazard_decay_s);
  yaml_get(root, "max_migration_queue", cfg.max_migration_queue);
  return cfg;
}

static Result<AgentConfig> parse_agent(const YAML::Node& root) {
  AgentConfig cfg;
  yaml_get(root, "node_id",               cfg.node_id);
  yaml_get(root, "building",              cfg.building);
  yaml_get(root, "switch_id",             cfg.switch_id);
  yaml_get(root, "coordinator_address",   cfg.coordinator_address);
  yaml_get(root, "listen_address",        cfg.listen_address);
  yaml_get(root, "total_vram_bytes",      cfg.total_vram_bytes);
  yaml_get(root, "link_capacity_bps",     cfg.link_capacity_bps);
  yaml_get(root, "iface",                 cfg.iface);
  yaml_get(root, "checkpoint_dir",        cfg.checkpoint_dir);
  yaml_get(root, "tls_cert_path",         cfg.tls_cert_path);
  yaml_get(root, "tls_key_path",          cfg.tls_key_path);
  yaml_get(root, "tls_ca_path",           cfg.tls_ca_path);
  yaml_get(root, "mtls_enabled",          cfg.mtls_enabled);
  yaml_get(root, "telemetry_interval_s",  cfg.telemetry_interval_s);
  yaml_get(root, "heartbeat_interval_s",  cfg.heartbeat_interval_s);
  yaml_get(root, "migration_recv_buf",    cfg.migration_recv_buf);
  return cfg;
}

#endif



Result<CoordinatorConfig> load_coordinator_config(const std::string& path) {
#if HAVE_YAML_CPP
  try {
    YAML::Node root = YAML::LoadFile(path);
    return parse_coordinator(root);
  } catch (const YAML::Exception& e) {
    return Error{"YAML parse error in " + path + ": " + e.what()};
  } catch (const std::exception& e) {
    return Error{"Failed to load coordinator config from " + path + ": " + e.what()};
  }
#else
  LOG_WARN("yaml-cpp not available; using default CoordinatorConfig (path=%s)", path.c_str());
  return CoordinatorConfig{};
#endif
}

Result<AgentConfig> load_agent_config(const std::string& path) {
#if HAVE_YAML_CPP
  try {
    YAML::Node root = YAML::LoadFile(path);
    return parse_agent(root);
  } catch (const YAML::Exception& e) {
    return Error{"YAML parse error in " + path + ": " + e.what()};
  } catch (const std::exception& e) {
    return Error{"Failed to load agent config from " + path + ": " + e.what()};
  }
#else
  LOG_WARN("yaml-cpp not available; using default AgentConfig (path=%s)", path.c_str());
  return AgentConfig{};
#endif
}

VoidResult validate_coordinator_config(const CoordinatorConfig& cfg) {
  if (cfg.listen_address.empty())
    return err_void("listen_address must not be empty");
  if (cfg.epoch_seconds <= 0.0)
    return err_void("epoch_seconds must be positive");
  if (cfg.B_ckpt_bps == 0)
    return err_void("B_ckpt_bps must be positive");
  if (cfg.migration_share <= 0.0 || cfg.migration_share > 1.0)
    return err_void("migration_share must be in (0,1]");
  if (cfg.heartbeat_timeout_s <= 0.0)
    return err_void("heartbeat_timeout_s must be positive");
  return ok_void();
}

VoidResult validate_agent_config(const AgentConfig& cfg) {
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
