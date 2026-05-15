#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace reclaimnet {
namespace control {

struct NodeTelemetry {
  double gpu_util_pct = 0;
  double vram_used_pct = 0;
  uint64_t vram_free_bytes = 0;
  double link_tx_bps = 0;
  double link_rx_bps = 0;
  double cpu_load_1m = 0;
  int64_t updated_ns = 0;
  uint32_t gpu_free_count = 1;
  double cuda_capability = 0.0;
};

struct NodeRecord {
  NodeId node_id;
  NodeState state = NodeState::UNKNOWN;
  std::string address;
  std::string building;
  std::string switch_id;
  uint64_t total_vram_bytes = 0;
  BandwidthBps link_capacity_bps = 0;
  int64_t last_heartbeat_ns = 0;
  int64_t registered_at_ns = 0;
  NodeTelemetry telemetry;
  uint32_t gpu_count = 1;
  uint32_t gpu_free_count = 1;
  double cuda_capability = 0.0;
  double historical_migration_cost_s = 0.0;
  int64_t historical_migration_cost_updated_ns = 0;
};

class NodeRegistry {
public:
  NodeRegistry() = default;

  void upsert(const NodeRecord &rec);
  void clear();

  std::optional<NodeRecord> get(const NodeId &id) const;

  VoidResult update_telemetry(const NodeId &id, const NodeTelemetry &tel);

  VoidResult heartbeat(const NodeId &id, int64_t now_ns);

  std::vector<NodeId> mark_stale(int64_t threshold_ns);

  std::vector<NodeRecord> by_state(NodeState s) const;

  std::vector<NodeRecord> all() const;

  size_t size() const;

  uint64_t free_vram(const NodeId &id) const;

private:
  mutable std::mutex mu_;
  std::unordered_map<NodeId, NodeRecord> nodes_;
};

}
}
