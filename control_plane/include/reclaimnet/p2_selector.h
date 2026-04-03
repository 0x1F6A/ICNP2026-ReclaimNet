#pragma once






#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/node_registry.h"

#include <vector>
#include <string>

namespace reclaimnet {
namespace control {


struct P2Candidate {
  NodeId   node_id;
  double   composite_cost;
  double   T_transfer_s;
  double   survival_prob;
  double   topology_dist;
  bool     feasible;
  std::string infeasible_reason;
};


struct P2Result {
  bool                    success;
  NodeId                  selected;
  std::vector<P2Candidate> ranked;
  std::string             reason;
};


struct P2Query {
  JobId    job_id;
  NodeId   source_node;
  uint64_t checkpoint_size_b;
  double   reclaim_deadline_s;
  double   T_restore_s;


  double   alpha = 0.0;
  double   beta  = 0.0;
  double   gamma = 0.0;
};


class P2Selector {
public:
  P2Selector(double alpha, double beta, double gamma,
             const TopologyGraph& topo,
             const HazardEstimator& hazard,
             const NodeRegistry& nodes);


  P2Result select(const P2Query& q, double horizon_s = 3600.0) const;


  static double topology_dist_score(TopologyLevel lvl) noexcept;

  void set_weights(double a, double b, double g) noexcept {
    alpha_ = a; beta_ = b; gamma_ = g;
  }

private:
  double alpha_, beta_, gamma_;
  const TopologyGraph&    topo_;
  const HazardEstimator&  hazard_;
  const NodeRegistry&     nodes_;

  P2Candidate evaluate(const NodeRecord& candidate,
                       const P2Query& q,
                       double eff_alpha,
                       double eff_beta,
                       double eff_gamma,
                       double horizon_s) const;
};

}
}
