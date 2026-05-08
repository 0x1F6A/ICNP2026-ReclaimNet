#include "reclaimnet/p2_selector.h"
#include "reclaimnet/capacity_estimator.h"
#include "reclaimnet/time.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>

namespace reclaimnet {
namespace control {

double P2Selector::topology_dist_score(TopologyLevel level) noexcept {
  switch (level) {
  case TopologyLevel::INTRA_RACK:
    return 0.0;
  case TopologyLevel::INTRA_BUILDING:
    return 0.3;
  case TopologyLevel::CROSS_BUILDING:
    return 0.7;
  case TopologyLevel::CORE_CROSSING:
    return 1.0;
  }
  return 1.0;
}

P2Selector::P2Selector(double alpha, double beta, double gamma,
                       const TopologyGraph &topo, const HazardEstimator &hazard,
                       const NodeRegistry &nodes,
                       const CapacityEstimator *capacities)
    : alpha_(alpha), beta_(beta), gamma_(gamma), topo_(topo), hazard_(hazard),
      nodes_(nodes), capacities_(capacities) {}

VoidResult P2Selector::observe_migration_cost(const NodeId &node,
                                              double duration_s,
                                              int64_t completed_ns) {
  if (node.empty() || !std::isfinite(duration_s) || duration_s < 0 ||
      completed_ns <= 0)
    return err_void("Invalid migration cost observation");
  std::lock_guard<std::mutex> lock(history_mu_);
  auto &samples = history_[node];
  auto position =
      std::lower_bound(samples.begin(), samples.end(), completed_ns,
                       [](const CostSample &sample, int64_t timestamp) {
                         return sample.completed_ns < timestamp;
                       });
  if (position != samples.end() && position->completed_ns == completed_ns)
    position->duration_s = duration_s;
  else
    samples.insert(position, {completed_ns, duration_s});
  return ok_void();
}

double P2Selector::historical_cost(const NodeRecord &node, const P2Query &query,
                                   int64_t decision_ns) const {
  std::lock_guard<std::mutex> lock(history_mu_);
  const auto found = history_.find(node.node_id);
  long double sum = 0;
  size_t count = 0;
  if (found != history_.end())
    for (const auto &sample : found->second) {
      if (sample.completed_ns > decision_ns)
        break;
      sum += sample.duration_s;
      ++count;
    }
  if (count > 0)
    return static_cast<double>(sum / count);
  if (std::isfinite(node.historical_migration_cost_s) &&
      node.historical_migration_cost_s > 0 &&
      node.historical_migration_cost_updated_ns <= decision_ns)
    return node.historical_migration_cost_s;
  return query.historical_cost_prior_s;
}

P2Candidate P2Selector::evaluate(const NodeRecord &node, const P2Query &query,
                                 double alpha, double horizon_s,
                                 int64_t decision_ns) const {
  P2Candidate candidate;
  candidate.node_id = node.node_id;
  candidate.composite_cost = std::numeric_limits<double>::infinity();
  auto reject = [&](const std::string &reason) {
    candidate.infeasible_reason = reason;
    return candidate;
  };
  uint32_t free_gpus = node.gpu_free_count;
  uint64_t free_vram = node.telemetry.vram_free_bytes;
  auto resources = query.destination_resources.find(node.node_id);
  if (resources != query.destination_resources.end()) {
    free_vram = std::min(free_vram, resources->second.first);
    free_gpus = std::min(free_gpus, resources->second.second);
  }
  if (node.gpu_count < query.gpu_count_required ||
      free_gpus < query.gpu_count_required)
    return reject("insufficient GPU count");
  if (!std::isfinite(node.cuda_capability) ||
      node.cuda_capability < query.cuda_capability_required)
    return reject("incompatible CUDA capability");
  if (!std::isfinite(node.telemetry.gpu_util_pct) ||
      node.telemetry.gpu_util_pct > query.load_limit_pct ||
      node.telemetry.gpu_util_pct < 0)
    return reject("GPU load exceeds limit");
  const uint64_t required_vram = query.vram_required_bytes > 0
                                     ? query.vram_required_bytes
                                     : query.checkpoint_size_b;
  if (free_vram < required_vram)
    return reject("insufficient VRAM");
  auto path = topo_.shortest_path(query.source_node, node.node_id);
  if (!path)
    return reject("no network path");
  candidate.path_nodes = path->path;
  double bottleneck = static_cast<double>(node.link_capacity_bps);
  const auto source = nodes_.get(query.source_node);
  if (source && source->link_capacity_bps > 0)
    bottleneck =
        std::min(bottleneck, static_cast<double>(source->link_capacity_bps));
  for (size_t i = 1; i < path->path.size(); ++i) {
    double capacity = 0;
    for (const auto &link : topo_.links_from(path->path[i - 1]))
      if (link.dst == path->path[i]) {
        double estimate =
            capacities_
                ? capacities_->lower_bound_bps(link.src, link.dst, decision_ns,
                                               link.bandwidth_bps)
                : link.bandwidth_bps;
        capacity = std::max(capacity, estimate);
      }
    const double reserved = std::max(query.research_min_bps,
                                     (1.0 - query.migration_share) * capacity);
    bottleneck = std::min(bottleneck, std::max(0.0, capacity - reserved));
  }
  if (!std::isfinite(bottleneck) || bottleneck <= 0)
    return reject("zero path capacity");
  candidate.bottleneck_bps = bottleneck;
  candidate.T_transfer_s = bytes_to_bits(query.checkpoint_size_b) / bottleneck;
  candidate.T_migration_s = candidate.T_transfer_s + query.T_restore_s;
  if (query.require_deadline &&
      candidate.T_migration_s > query.reclaim_deadline_s)
    return reject("transfer+restore exceeds deadline");
  const double lambda =
      hazard_.estimate_at(node.node_id, decision_ns).value_or(0.0);
  candidate.survival_prob = std::exp(-lambda * horizon_s);
  candidate.historical_migration_cost_s =
      historical_cost(node, query, decision_ns);
  candidate.composite_cost =
      candidate.T_migration_s + alpha * (1.0 - candidate.survival_prob) *
                                    candidate.historical_migration_cost_s;
  candidate.topology_dist = topology_dist_score(path->max_level);
  candidate.same_building =
      source && !source->building.empty() && source->building == node.building;
  candidate.feasible = true;
  return candidate;
}

P2Result P2Selector::select(const P2Query &query, double horizon_s) const {
  P2Result result;
  const double alpha = query.alpha > 0 ? query.alpha : alpha_;
  if (horizon_s < 0)
    horizon_s = query.remaining_runtime_s;
  if (query.source_node.empty() || query.checkpoint_size_b == 0 ||
      !std::isfinite(alpha) || alpha < 0 || !std::isfinite(horizon_s) ||
      horizon_s < 0 || !std::isfinite(query.T_restore_s) ||
      query.T_restore_s < 0 || !std::isfinite(query.migration_share) ||
      query.migration_share < 0 || query.migration_share > 1 ||
      !std::isfinite(query.research_min_bps) || query.research_min_bps < 0 ||
      !std::isfinite(query.load_limit_pct) || query.load_limit_pct < 0 ||
      query.load_limit_pct > 100 ||
      !std::isfinite(query.cuda_capability_required) ||
      query.cuda_capability_required < 0 ||
      !std::isfinite(query.historical_cost_prior_s) ||
      query.historical_cost_prior_s < 0 ||
      (query.require_deadline && (!std::isfinite(query.reclaim_deadline_s) ||
                                  query.reclaim_deadline_s <= 0))) {
    result.reason = "Invalid P2 query";
    return result;
  }
  const int64_t decision_ns =
      query.decision_ns > 0 ? query.decision_ns : now_ns();
  size_t local_count = 0;
  for (const auto &node : nodes_.by_state(NodeState::ONLINE)) {
    if (node.node_id == query.source_node)
      continue;
    auto candidate = evaluate(node, query, alpha, horizon_s, decision_ns);
    if (candidate.feasible && candidate.same_building)
      ++local_count;
    result.ranked.push_back(std::move(candidate));
  }
  const bool local_pool = query.k_min > 0 && local_count >= query.k_min;
  for (auto &candidate : result.ranked) {
    candidate.in_selection_pool =
        candidate.feasible && (!local_pool || candidate.same_building);
    if (candidate.feasible && !candidate.in_selection_pool) {
      candidate.feasible = false;
      candidate.infeasible_reason = "excluded by hierarchical locality filter";
    }
  }
  std::sort(result.ranked.begin(), result.ranked.end(),
            [](const P2Candidate &a, const P2Candidate &b) {
              if (a.feasible != b.feasible)
                return a.feasible;
              if (a.composite_cost != b.composite_cost)
                return a.composite_cost < b.composite_cost;
              return a.node_id < b.node_id;
            });
  if (!result.ranked.empty() && result.ranked.front().feasible) {
    result.success = true;
    result.selected = result.ranked.front().node_id;
  } else
    result.reason = "No feasible destination found for job " + query.job_id;
  return result;
}

P2GangResult P2Selector::select_gang(const P2GangQuery &query) const {
  P2GangResult result;
  const size_t ranks = query.source_nodes.size();
  if (ranks == 0 ||
      (!query.rank_queries.empty() && query.rank_queries.size() != ranks)) {
    result.reason = "Invalid distributed rank specification";
    return result;
  }
  std::map<NodeId, size_t> destinations;
  for (size_t rank = 0; rank < ranks; ++rank) {
    auto single = query.rank_queries.empty() ? query.rank_query
                                             : query.rank_queries[rank];
    single.source_node = query.source_nodes[rank];
    auto selected = select(single);
    for (auto &candidate : selected.ranked) {
      if (std::find(query.source_nodes.begin(), query.source_nodes.end(),
                    candidate.node_id) != query.source_nodes.end()) {
        candidate.feasible = false;
        candidate.infeasible_reason = "distributed source is not a destination";
      }
      if (candidate.feasible)
        destinations.emplace(candidate.node_id, 0);
    }
    result.ranks.push_back(std::move(selected));
  }
  size_t index = 0;
  for (auto &[_, value] : destinations)
    value = index++;
  struct Edge {
    size_t dst;
    size_t reverse;
    int capacity;
    double cost;
  };
  const size_t sink = 1 + ranks + destinations.size();
  std::vector<std::vector<Edge>> graph(sink + 1);
  auto edge = [&](size_t source, size_t dst, int capacity, double cost) {
    graph[source].push_back({dst, graph[dst].size(), capacity, cost});
    graph[dst].push_back({source, graph[source].size() - 1, 0, -cost});
  };
  for (size_t rank = 0; rank < ranks; ++rank) {
    edge(0, 1 + rank, 1, 0);
    for (const auto &candidate : result.ranks[rank].ranked)
      if (candidate.feasible)
        edge(1 + rank, 1 + ranks + destinations.at(candidate.node_id), 1,
             candidate.composite_cost);
  }
  for (const auto &[_, destination] : destinations)
    edge(1 + ranks + destination, sink, 1, 0);
  for (size_t flow = 0; flow < ranks; ++flow) {
    std::vector<double> distance(graph.size(),
                                 std::numeric_limits<double>::infinity());
    std::vector<size_t> previous_node(graph.size()),
        previous_edge(graph.size());
    distance[0] = 0;
    for (size_t pass = 1; pass < graph.size(); ++pass) {
      bool changed = false;
      for (size_t node = 0; node < graph.size(); ++node) {
        if (!std::isfinite(distance[node]))
          continue;
        for (size_t e = 0; e < graph[node].size(); ++e) {
          const auto &option = graph[node][e];
          if (option.capacity > 0 &&
              distance[node] + option.cost < distance[option.dst] - 1e-10) {
            distance[option.dst] = distance[node] + option.cost;
            previous_node[option.dst] = node;
            previous_edge[option.dst] = e;
            changed = true;
          }
        }
      }
      if (!changed)
        break;
    }
    if (!std::isfinite(distance[sink])) {
      result.reason = "No complete compatible distributed placement";
      return result;
    }
    for (size_t node = sink; node != 0; node = previous_node[node]) {
      auto &option = graph[previous_node[node]][previous_edge[node]];
      --option.capacity;
      ++graph[node][option.reverse].capacity;
    }
  }
  result.selected.resize(ranks);
  for (size_t rank = 0; rank < ranks; ++rank)
    for (const auto &option : graph[1 + rank])
      if (option.dst > ranks && option.dst < sink && option.capacity == 0)
        for (const auto &[node, destination] : destinations)
          if (option.dst == 1 + ranks + destination) {
            result.selected[rank] = node;
            result.ranks[rank].success = true;
            result.ranks[rank].selected = node;
          }
  result.success = true;
  return result;
}

}
}
