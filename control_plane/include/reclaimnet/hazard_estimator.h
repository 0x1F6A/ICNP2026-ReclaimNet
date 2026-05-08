#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <deque>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

namespace reclaimnet {
namespace control {

struct ReclaimEvent {
  NodeId node_id;
  int64_t timestamp_ns;
  ReclaimType type;
};

struct NodeHazard {
  double lambda = 0.0;
  int64_t last_event_ns = 0;
  uint64_t event_count = 0;
  int64_t last_sample_ns = 0;
  bool confidence_available = false;
};

class HazardEstimator {
public:
  explicit HazardEstimator(double decay_half_life_s = 3600.0);
  void configure_window(double window_s, double z = 1.96);
  VoidResult observe_node(const NodeId &node_id, int64_t timestamp_ns);
  void record_event(const ReclaimEvent &ev);
  VoidResult observe_sample(const NodeId &node_id, double lambda_e,
                            int64_t timestamp_ns);
  std::optional<double> estimate_at(const NodeId &node_id,
                                    int64_t decision_ns) const;
  std::optional<NodeHazard> summary_at(const NodeId &node_id,
                                       int64_t decision_ns) const;
  std::optional<double> confidence_upper_bound(const NodeId &node_id,
                                               int64_t decision_ns) const;
  double upper_bound(const NodeId &node_id, int64_t decision_ns) const;
  std::optional<double> estimate(const NodeId &node_id) const;
  double lambda(const NodeId &node_id) const;
  double survival_prob(const NodeId &node_id, double horizon_s = 3600.0) const;
  std::unordered_map<NodeId, NodeHazard> snapshot() const;
  void prune(double max_age_s = 86400.0);

private:
  struct Sample {
    int64_t timestamp_ns;
    double lambda;
  };
  struct History {
    int64_t observed_since_ns = 0;
    int64_t last_observation_ns = 0;
    std::deque<int64_t> events;
    std::deque<Sample> samples;
  };
  struct Estimate {
    NodeHazard hazard;
    double exposure_s;
  };
  std::optional<Estimate> estimate_locked(const NodeId &node_id,
                                          int64_t decision_ns) const;
  double window_s_;
  int64_t window_ns_;
  double confidence_z_ = 1.96;
  mutable std::shared_mutex mu_;
  std::unordered_map<NodeId, History> state_;
};

}
}
