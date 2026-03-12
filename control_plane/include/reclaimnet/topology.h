#pragma once
/// @file topology.h
/// @brief Network topology graph: nodes, links, Dijkstra shortest-path.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <string>
#include <optional>

namespace reclaimnet {
namespace control {

/// One directed link in the topology graph.
struct Link {
  NodeId   src;
  NodeId   dst;
  double   bandwidth_bps;   ///< Physical capacity
  double   latency_us;      ///< One-way propagation delay
  TopologyLevel level;      ///< PCIe / NVLink / IntraRack / InterRack / WAN
};

/// Shortest-path result between two nodes.
struct PathResult {
  std::vector<NodeId>   path;          ///< src → ... → dst
  double                total_latency_us;
  double                bottleneck_bps;
  TopologyLevel         max_level;     ///< highest (most expensive) hop
};

/// Immutable snapshot of the network topology loaded from file.
class TopologyGraph {
public:
  /// Construct an empty graph.
  TopologyGraph() = default;

  /// Load topology from a simple JSON/YAML adjacency file.
  static Result<TopologyGraph> load_from_file(const std::string& path);

  /// Add a bidirectional link (convenience for tests).
  void add_link(const Link& link);

  /// Dijkstra shortest path weighted by latency.
  /// Returns empty optional when no path exists.
  std::optional<PathResult> shortest_path(const NodeId& src, const NodeId& dst) const;

  /// Topology-level distance between two nodes (for P2 γ term).
  /// Returns TopologyLevel::InterRack when unknown.
  TopologyLevel level_between(const NodeId& a, const NodeId& b) const;

  /// All node IDs present in the graph.
  std::vector<NodeId> all_nodes() const;

  /// Retrieve the direct links for a given node.
  const std::vector<Link>& links_from(const NodeId& n) const;

  size_t node_count()  const noexcept { return adj_.size(); }
  size_t link_count()  const noexcept { return link_count_; }

private:
  // Adjacency list: node → outgoing links.
  std::unordered_map<NodeId, std::vector<Link>> adj_;
  size_t link_count_ = 0;

  // Cached direct link lookup (pair → min-latency link).
  mutable std::unordered_map<std::string, Link> direct_cache_;
  static const std::vector<Link> empty_links_;
};

}  // namespace control
}  // namespace reclaimnet
