#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <optional>
#include <string>

namespace reclaimnet {
namespace control {


struct NodeTelemetry {
  double   gpu_util_pct;
  double   vram_used_pct;
  uint64_t vram_free_bytes;
  double   link_tx_bps;
  double   link_rx_bps;
  double   cpu_load_1m;
  int64_t  updated_ns;
};


struct NodeRecord {
  NodeId         node_id;
  NodeState      state;
  std::string    address;
  std::string    building;
  std::string    switch_id;
  uint64_t       total_vram_bytes;
  BandwidthBps   link_capacity_bps;
  int64_t        last_heartbeat_ns;
  int64_t        registered_at_ns;
  NodeTelemetry  telemetry;
};


class NodeRegistry {
public:
  NodeRegistry() = default;


  void upsert(const NodeRecord& rec);


  std::optional<NodeRecord> get(const NodeId& id) const;


  VoidResult update_telemetry(const NodeId& id, const NodeTelemetry& tel);


  VoidResult heartbeat(const NodeId& id, int64_t now_ns);



  std::vector<NodeId> mark_stale(int64_t threshold_ns);


  std::vector<NodeRecord> by_state(NodeState s) const;


  std::vector<NodeRecord> all() const;


  size_t size() const;


  uint64_t free_vram(const NodeId& id) const;

private:
  mutable std::mutex mu_;
  std::unordered_map<NodeId, NodeRecord> nodes_;
};

}
}
