#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <deque>
#include <map>
#include <shared_mutex>
#include <utility>

namespace reclaimnet {
namespace control {

class CapacityEstimator {
public:
  explicit CapacityEstimator(double window_s = 300.0,
                             double confidence_factor = 0.8);
  VoidResult observe_sample(const NodeId &src, const NodeId &dst,
                            double capacity_bps, int64_t timestamp_ns);
  double lower_bound_bps(const NodeId &src, const NodeId &dst,
                         int64_t decision_ns, double nominal_bps) const;

private:
  struct Sample {
    int64_t timestamp_ns;
    double capacity_bps;
  };
  int64_t window_ns_;
  double confidence_factor_;
  mutable std::shared_mutex mu_;
  std::map<std::pair<NodeId, NodeId>, std::deque<Sample>> samples_;
};

}
}
