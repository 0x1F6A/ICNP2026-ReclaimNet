#include "reclaimnet/capacity_estimator.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <stdexcept>

namespace reclaimnet {
namespace control {

CapacityEstimator::CapacityEstimator(double window_s, double confidence_factor)
    : confidence_factor_(confidence_factor) {
  const long double window_ns = static_cast<long double>(window_s) * 1e9L;
  if (!std::isfinite(window_s) || window_ns < 1.0L ||
      window_ns > static_cast<long double>(std::numeric_limits<int64_t>::max()))
    throw std::invalid_argument(
        "Capacity observation window must be positive and representable");
  if (!std::isfinite(confidence_factor) || confidence_factor <= 0.0 ||
      confidence_factor > 1.0)
    throw std::invalid_argument("Capacity confidence factor must be in (0, 1]");
  window_ns_ = static_cast<int64_t>(window_ns);
}

VoidResult CapacityEstimator::observe_sample(const NodeId &src,
                                             const NodeId &dst,
                                             double capacity_bps,
                                             int64_t timestamp_ns) {
  if (src.empty() || dst.empty() || src == dst ||
      !std::isfinite(capacity_bps) || capacity_bps < 0.0 || timestamp_ns <= 0)
    return err_void("Invalid capacity sample");
  std::unique_lock<std::shared_mutex> lock(mu_);
  auto &samples = samples_[{src, dst}];
  const auto position =
      std::lower_bound(samples.begin(), samples.end(), timestamp_ns,
                       [](const Sample &sample, int64_t stamp) {
                         return sample.timestamp_ns < stamp;
                       });
  if (position != samples.end() && position->timestamp_ns == timestamp_ns)
    position->capacity_bps = std::min(position->capacity_bps, capacity_bps);
  else
    samples.insert(position, {timestamp_ns, capacity_bps});
  return ok_void();
}

double CapacityEstimator::lower_bound_bps(const NodeId &src, const NodeId &dst,
                                          int64_t decision_ns,
                                          double nominal_bps) const {
  if (src.empty() || dst.empty() || src == dst || decision_ns <= 0 ||
      !std::isfinite(nominal_bps) || nominal_bps < 0.0)
    return 0.0;
  std::shared_lock<std::shared_mutex> lock(mu_);
  const auto found = samples_.find({src, dst});
  if (found == samples_.end())
    return nominal_bps;
  const auto cutoff_ns =
      decision_ns > window_ns_ ? decision_ns - window_ns_ : 0;
  const auto &samples = found->second;
  const auto first = std::upper_bound(samples.begin(), samples.end(), cutoff_ns,
                                      [](int64_t stamp, const Sample &sample) {
                                        return stamp < sample.timestamp_ns;
                                      });
  double minimum = std::numeric_limits<double>::infinity();
  for (auto sample = first;
       sample != samples.end() && sample->timestamp_ns <= decision_ns; ++sample)
    minimum = std::min(minimum, sample->capacity_bps);
  if (!std::isfinite(minimum))
    return nominal_bps;
  return std::min(nominal_bps, minimum * confidence_factor_);
}

}
}
