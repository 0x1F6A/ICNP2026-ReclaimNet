#pragma once

#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/types.h"

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace reclaimnet {
namespace control {

class CapacityEstimator;

struct P2Candidate {
  NodeId node_id;
  double composite_cost = 0.0;
  double T_transfer_s = 0.0;
  double survival_prob = 0.0;
  double topology_dist = 0.0;
  bool feasible = false;
  std::string infeasible_reason;
  double T_migration_s = 0.0;
  double historical_migration_cost_s = 0.0;
  double bottleneck_bps = 0.0;
  bool same_building = false;
  bool in_selection_pool = false;
  std::vector<NodeId> path_nodes;
};

struct P2Result {
  bool success = false;
  NodeId selected;
  std::vector<P2Candidate> ranked;
  std::string reason;
};

struct P2Query {
  JobId job_id;
  NodeId source_node;
  uint64_t checkpoint_size_b = 0;
  double reclaim_deadline_s = 0.0;
  double T_restore_s = 0.0;
  double alpha = 0.0;
  double beta = 0.0;
  double gamma = 0.0;
  uint64_t vram_required_bytes = 0;
  double migration_share = 1.0;
  double research_min_bps = 0.0;
  double remaining_runtime_s = 3600.0;
  double cuda_capability_required = 0.0;
  uint32_t gpu_count_required = 1;
  double load_limit_pct = 80.0;
  size_t k_min = 2;
  int64_t decision_ns = 0;
  double historical_cost_prior_s = 30.0;
  bool require_deadline = true;
  std::unordered_map<NodeId, std::pair<uint64_t, uint32_t>>
      destination_resources;
};

struct P2GangQuery {
  std::vector<NodeId> source_nodes;
  P2Query rank_query;
  std::vector<P2Query> rank_queries;
};

struct P2GangResult {
  bool success = false;
  std::vector<NodeId> selected;
  std::vector<P2Result> ranks;
  std::string reason;
};

class P2Selector {
public:
  P2Selector(double alpha, double beta, double gamma, const TopologyGraph &topo,
             const HazardEstimator &hazard, const NodeRegistry &nodes,
             const CapacityEstimator *capacities = nullptr);
  P2Result select(const P2Query &q, double horizon_s = -1.0) const;
  P2GangResult select_gang(const P2GangQuery &query) const;
  static double topology_dist_score(TopologyLevel lvl) noexcept;
  void set_weights(double a, double b, double g) noexcept {
    alpha_ = a;
    beta_ = b;
    gamma_ = g;
  }
  VoidResult observe_migration_cost(const NodeId &node, double duration_s,
                                    int64_t completed_ns);

private:
  struct CostSample {
    int64_t completed_ns;
    double duration_s;
  };
  double alpha_, beta_, gamma_;
  const TopologyGraph &topo_;
  const HazardEstimator &hazard_;
  const NodeRegistry &nodes_;
  const CapacityEstimator *capacities_;
  mutable std::mutex history_mu_;
  std::unordered_map<NodeId, std::vector<CostSample>> history_;
  double historical_cost(const NodeRecord &node, const P2Query &query,
                         int64_t decision_ns) const;
  P2Candidate evaluate(const NodeRecord &node, const P2Query &query,
                       double alpha, double horizon_s,
                       int64_t decision_ns) const;
};

}
}
