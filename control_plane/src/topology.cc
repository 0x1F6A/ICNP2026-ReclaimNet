


#include "reclaimnet/topology.h"
#include "reclaimnet/log.h"

#include <queue>
#include <limits>
#include <algorithm>
#include <fstream>
#include <sstream>


#if __has_include(<nlohmann/json.hpp>)
#  include <nlohmann/json.hpp>
#  define HAVE_JSON 1
#else
#  define HAVE_JSON 0
#endif

namespace reclaimnet {
namespace control {

const std::vector<Link> TopologyGraph::empty_links_{};



void TopologyGraph::add_link(const Link& link) {
  adj_[link.src].push_back(link);

  Link rev = link;
  std::swap(rev.src, rev.dst);
  adj_[link.dst].push_back(rev);
  link_count_ += 2;
}



const std::vector<Link>& TopologyGraph::links_from(const NodeId& n) const {
  auto it = adj_.find(n);
  if (it == adj_.end()) return empty_links_;
  return it->second;
}



std::vector<NodeId> TopologyGraph::all_nodes() const {
  std::vector<NodeId> out;
  out.reserve(adj_.size());
  for (auto& [k, _] : adj_) out.push_back(k);
  return out;
}



std::optional<PathResult> TopologyGraph::shortest_path(
    const NodeId& src, const NodeId& dst) const {

  if (src == dst) {
    return PathResult{{src, dst}, 0.0, std::numeric_limits<double>::max(), TopologyLevel::NVLink};
  }

  using Pair = std::pair<double, NodeId>;
  std::priority_queue<Pair, std::vector<Pair>, std::greater<Pair>> pq;
  std::unordered_map<NodeId, double>  dist;
  std::unordered_map<NodeId, NodeId>  prev;
  std::unordered_map<NodeId, double>  bottleneck;

  dist[src]      = 0.0;
  bottleneck[src] = std::numeric_limits<double>::max();
  pq.emplace(0.0, src);

  while (!pq.empty()) {
    auto [d, u] = pq.top(); pq.pop();
    if (d > dist[u] + 1e-9) continue;
    if (u == dst) break;

    for (const Link& lk : links_from(u)) {
      double nd = dist[u] + lk.latency_us;
      auto it = dist.find(lk.dst);
      if (it == dist.end() || nd < it->second) {
        dist[lk.dst]      = nd;
        prev[lk.dst]      = u;
        bottleneck[lk.dst] = std::min(bottleneck[u], lk.bandwidth_bps);
        pq.emplace(nd, lk.dst);
      }
    }
  }

  if (dist.find(dst) == dist.end()) return std::nullopt;


  std::vector<NodeId> path;
  for (NodeId cur = dst; cur != src; cur = prev.at(cur))
    path.push_back(cur);
  path.push_back(src);
  std::reverse(path.begin(), path.end());


  TopologyLevel max_lvl = TopologyLevel::NVLink;
  for (size_t i = 0; i + 1 < path.size(); ++i) {
    max_lvl = std::max(max_lvl, level_between(path[i], path[i+1]));
  }

  return PathResult{path, dist.at(dst), bottleneck.at(dst), max_lvl};
}



TopologyLevel TopologyGraph::level_between(const NodeId& a, const NodeId& b) const {
  auto key = a + ":" + b;

  {
    auto it = direct_cache_.find(key);
    if (it != direct_cache_.end()) return it->second.level;
  }

  for (const Link& lk : links_from(a)) {
    if (lk.dst == b) {
      direct_cache_[key] = lk;
      return lk.level;
    }
  }
  return TopologyLevel::InterRack;
}



Result<TopologyGraph> TopologyGraph::load_from_file(const std::string& path) {
#if HAVE_JSON
  std::ifstream ifs(path);
  if (!ifs.is_open()) return Error{"Cannot open topology file: " + path};

  try {
    nlohmann::json j;
    ifs >> j;
    TopologyGraph g;
    for (auto& lk_j : j["links"]) {
      Link lk;
      lk.src           = lk_j["src"].get<std::string>();
      lk.dst           = lk_j["dst"].get<std::string>();
      lk.bandwidth_bps = lk_j.value("bandwidth_bps", 1e10);
      lk.latency_us    = lk_j.value("latency_us", 1.0);
      std::string lvl  = lk_j.value("level", "IntraRack");
      if      (lvl == "NVLink")    lk.level = TopologyLevel::NVLink;
      else if (lvl == "PCIe")      lk.level = TopologyLevel::PCIe;
      else if (lvl == "IntraRack") lk.level = TopologyLevel::IntraRack;
      else if (lvl == "InterRack") lk.level = TopologyLevel::InterRack;
      else                         lk.level = TopologyLevel::WAN;
      g.add_link(lk);
    }
    LOG_INFO("Loaded topology from %s: %zu nodes, %zu links",
             path.c_str(), g.node_count(), g.link_count());
    return g;
  } catch (const std::exception& e) {
    return Error{std::string("Topology parse error: ") + e.what()};
  }
#else
  LOG_WARN("nlohmann/json not available; returning empty topology for %s", path.c_str());
  return TopologyGraph{};
#endif
}

}
}
