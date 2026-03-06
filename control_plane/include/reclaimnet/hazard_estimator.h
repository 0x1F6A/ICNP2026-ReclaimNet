#pragma once
/// @file hazard_estimator.h
/// @brief Non-homogeneous Poisson process (NHPP) hazard-rate estimator.
///
/// Estimates λ_e(t) for each node – the instantaneous reclaim arrival rate
/// used by P1 (Δt* formula) and P2 (survival probability).

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <deque>
#include <chrono>
#include <mutex>

namespace reclaimnet {
namespace control {

/// An individual reclaim event observation.
struct ReclaimEvent {
  NodeId   node_id;
  int64_t  timestamp_ns;   ///< wall-clock nanoseconds
  ReclaimType type;
};

/// Per-node NHPP state using a sliding exponential decay estimator.
struct NodeHazard {
  double   lambda;        ///< estimated hazard rate (events/second)
  int64_t  last_event_ns; ///< timestamp of most-recent event
  uint64_t event_count;   ///< total events seen
};

/// Thread-safe hazard-rate estimator.
class HazardEstimator {
public:
  explicit HazardEstimator(double decay_half_life_s = 3600.0);

  /// Record a new reclaim event and update λ_e for that node.
  void record_event(const ReclaimEvent& ev);

  /// Query the current hazard rate for a node (events/second).
  /// Returns 0 if node has never been seen.
  double lambda(const NodeId& node_id) const;

  /// Survival probability over horizon_s seconds.
  /// survival_prob = exp(-λ · horizon_s)
  double survival_prob(const NodeId& node_id, double horizon_s = 3600.0) const;

  /// Snapshot of all known hazard states.
  std::unordered_map<NodeId, NodeHazard> snapshot() const;

  /// Prune entries older than max_age_s (keeps memory bounded).
  void prune(double max_age_s = 86400.0);

private:
  double decay_alpha_;  ///< EWMA decay factor per second
  mutable std::mutex mu_;
  std::unordered_map<NodeId, NodeHazard> state_;
};

}  // namespace control
}  // namespace reclaimnet
