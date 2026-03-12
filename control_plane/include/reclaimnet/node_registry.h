#pragma once
/// @file node_registry.h
/// @brief In-memory registry of participating GPU nodes and their telemetry.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <optional>
#include <string>

namespace reclaimnet {
namespace control {

/// Live telemetry snapshot from a single node.
struct NodeTelemetry {
  double   gpu_util_pct;        ///< 0.0 – 100.0
  double   vram_used_pct;       ///< 0.0 – 100.0
  uint64_t vram_free_bytes;
  double   link_tx_bps;
  double   link_rx_bps;
  double   cpu_load_1m;
  int64_t  updated_ns;          ///< wall-clock timestamp of this snapshot
};

/// Full node descriptor stored in the registry.
struct NodeRecord {
  NodeId         node_id;
  NodeState      state;
  std::string    address;        ///< gRPC endpoint (host:port)
  std::string    building;
  std::string    switch_id;
  uint64_t       total_vram_bytes;
  BandwidthBps   link_capacity_bps;
  int64_t        last_heartbeat_ns;
  int64_t        registered_at_ns;
  NodeTelemetry  telemetry;
};

/// Thread-safe node registry.
class NodeRegistry {
public:
  NodeRegistry() = default;

  /// Register or update a node (upsert semantics).
  void upsert(const NodeRecord& rec);

  /// Look up a node by ID.
  std::optional<NodeRecord> get(const NodeId& id) const;

  /// Update telemetry for a known node.
  VoidResult update_telemetry(const NodeId& id, const NodeTelemetry& tel);

  /// Update heartbeat timestamp and set state ACTIVE.
  VoidResult heartbeat(const NodeId& id, int64_t now_ns);

  /// Mark stale nodes (last_heartbeat older than threshold_ns) as UNREACHABLE.
  /// Returns list of nodes that changed state.
  std::vector<NodeId> mark_stale(int64_t threshold_ns);

  /// All nodes in the given state.
  std::vector<NodeRecord> by_state(NodeState s) const;

  /// All registered nodes.
  std::vector<NodeRecord> all() const;

  /// Number of registered nodes.
  size_t size() const;

  /// Available free VRAM on a node (0 if unknown).
  uint64_t free_vram(const NodeId& id) const;

private:
  mutable std::mutex mu_;
  std::unordered_map<NodeId, NodeRecord> nodes_;
};

}  // namespace control
}  // namespace reclaimnet
