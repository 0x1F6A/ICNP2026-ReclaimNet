#pragma once
/// @file p2_selector.h
/// @brief P2 – Volatility-Aware Migration Destination Selection.
///
/// composite_cost = α·T_transfer + β·(1 - survival_prob) + γ·topology_dist
/// infeasible(τ) when C/B + T_r > τ  (reclaim deadline τ)

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/node_registry.h"

#include <vector>
#include <string>

namespace reclaimnet {
namespace control {

/// Candidate destination evaluated during P2.
struct P2Candidate {
  NodeId   node_id;
  double   composite_cost;
  double   T_transfer_s;
  double   survival_prob;
  double   topology_dist;    ///< normalised [0,1] from TopologyLevel
  bool     feasible;
  std::string infeasible_reason;
};

/// Full P2 selection result.
struct P2Result {
  bool                    success;
  NodeId                  selected;
  std::vector<P2Candidate> ranked;   ///< all candidates, best-first
  std::string             reason;    ///< failure reason if !success
};

/// Parameters for one P2 invocation.
struct P2Query {
  JobId    job_id;
  NodeId   source_node;
  uint64_t checkpoint_size_b;    ///< C_size
  double   reclaim_deadline_s;   ///< τ (seconds until mandatory reclaim)
  double   T_restore_s;          ///< T_r (estimated restore time)

  // P2 weights (override coordinator defaults when non-zero).
  double   alpha = 0.0;
  double   beta  = 0.0;
  double   gamma = 0.0;
};

/// Stateless P2 selector.  Thread-safe.
class P2Selector {
public:
  P2Selector(double alpha, double beta, double gamma,
             const TopologyGraph& topo,
             const HazardEstimator& hazard,
             const NodeRegistry& nodes);

  /// Select best migration destination.
  P2Result select(const P2Query& q, double horizon_s = 3600.0) const;

  /// Normalise a TopologyLevel to a [0,1] distance score.
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

}  // namespace control
}  // namespace reclaimnet
