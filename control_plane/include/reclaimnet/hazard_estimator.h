#pragma once






#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <deque>
#include <chrono>
#include <mutex>

namespace reclaimnet {
namespace control {


struct ReclaimEvent {
  NodeId   node_id;
  int64_t  timestamp_ns;
  ReclaimType type;
};


struct NodeHazard {
  double   lambda;
  int64_t  last_event_ns;
  uint64_t event_count;
};


class HazardEstimator {
public:
  explicit HazardEstimator(double decay_half_life_s = 3600.0);


  void record_event(const ReclaimEvent& ev);



  double lambda(const NodeId& node_id) const;



  double survival_prob(const NodeId& node_id, double horizon_s = 3600.0) const;


  std::unordered_map<NodeId, NodeHazard> snapshot() const;


  void prune(double max_age_s = 86400.0);

private:
  double decay_alpha_;
  mutable std::mutex mu_;
  std::unordered_map<NodeId, NodeHazard> state_;
};

}
}
