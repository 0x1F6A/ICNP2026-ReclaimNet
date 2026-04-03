#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <string>
#include <optional>

namespace reclaimnet {
namespace control {


struct Link {
  NodeId   src;
  NodeId   dst;
  double   bandwidth_bps;
  double   latency_us;
  TopologyLevel level;
};


struct PathResult {
  std::vector<NodeId>   path;
  double                total_latency_us;
  double                bottleneck_bps;
  TopologyLevel         max_level;
};


class TopologyGraph {
public:

  TopologyGraph() = default;


  static Result<TopologyGraph> load_from_file(const std::string& path);


  void add_link(const Link& link);



  std::optional<PathResult> shortest_path(const NodeId& src, const NodeId& dst) const;



  TopologyLevel level_between(const NodeId& a, const NodeId& b) const;


  std::vector<NodeId> all_nodes() const;


  const std::vector<Link>& links_from(const NodeId& n) const;

  size_t node_count()  const noexcept { return adj_.size(); }
  size_t link_count()  const noexcept { return link_count_; }

private:

  std::unordered_map<NodeId, std::vector<Link>> adj_;
  size_t link_count_ = 0;


  mutable std::unordered_map<std::string, Link> direct_cache_;
  static const std::vector<Link> empty_links_;
};

}
}
