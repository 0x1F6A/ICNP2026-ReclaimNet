#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/time.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <stdexcept>

namespace reclaimnet {
namespace control {
namespace {

int64_t validated_window_ns(double window_s) {
  const long double ns = static_cast<long double>(window_s) * 1e9L;
  if (!std::isfinite(window_s) || ns < 1.0L ||
      ns > static_cast<long double>(std::numeric_limits<int64_t>::max()))
    throw std::invalid_argument(
        "Hazard observation window must be positive and representable");
  return static_cast<int64_t>(ns);
}

}

HazardEstimator::HazardEstimator(double decay_half_life_s)
    : window_s_(decay_half_life_s),
      window_ns_(validated_window_ns(decay_half_life_s)) {}

void HazardEstimator::configure_window(double window_s, double z) {
  const auto window_ns = validated_window_ns(window_s);
  if (!std::isfinite(z) || z < 0.0 ||
      z > std::sqrt(std::numeric_limits<double>::max()))
    throw std::invalid_argument(
        "Hazard confidence multiplier must be finite and nonnegative");
  std::unique_lock<std::shared_mutex> lock(mu_);
  window_s_ = window_s;
  window_ns_ = window_ns;
  confidence_z_ = z;
}

VoidResult HazardEstimator::observe_node(const NodeId &node_id,
                                         int64_t timestamp_ns) {
  if (node_id.empty() || timestamp_ns <= 0)
    return err_void("Invalid hazard observation start");
  std::unique_lock<std::shared_mutex> lock(mu_);
  auto &history = state_[node_id];
  if (history.observed_since_ns == 0 ||
      timestamp_ns < history.observed_since_ns)
    history.observed_since_ns = timestamp_ns;
  history.last_observation_ns =
      std::max(history.last_observation_ns, timestamp_ns);
  return ok_void();
}

void HazardEstimator::record_event(const ReclaimEvent &ev) {
  if (ev.type != ReclaimType::EMERGENCY || ev.node_id.empty() ||
      ev.timestamp_ns <= 0)
    return;
  std::unique_lock<std::shared_mutex> lock(mu_);
  auto &history = state_[ev.node_id];
  if (history.observed_since_ns == 0 ||
      ev.timestamp_ns < history.observed_since_ns)
    history.observed_since_ns = ev.timestamp_ns;
  const auto position = std::lower_bound(history.events.begin(),
                                         history.events.end(), ev.timestamp_ns);
  if (position == history.events.end() || *position != ev.timestamp_ns)
    history.events.insert(position, ev.timestamp_ns);
}

VoidResult HazardEstimator::observe_sample(const NodeId &node_id,
                                           double lambda_e,
                                           int64_t timestamp_ns) {
  if (node_id.empty() || !std::isfinite(lambda_e) || lambda_e < 0.0 ||
      timestamp_ns <= 0)
    return err_void("Invalid trace hazard sample");
  std::unique_lock<std::shared_mutex> lock(mu_);
  auto &history = state_[node_id];
  if (history.observed_since_ns == 0 ||
      timestamp_ns < history.observed_since_ns)
    history.observed_since_ns = timestamp_ns;
  const auto position =
      std::lower_bound(history.samples.begin(), history.samples.end(),
                       timestamp_ns, [](const Sample &sample, int64_t stamp) {
                         return sample.timestamp_ns < stamp;
                       });
  if (position != history.samples.end() &&
      position->timestamp_ns == timestamp_ns)
    position->lambda = lambda_e;
  else
    history.samples.insert(position, {timestamp_ns, lambda_e});
  return ok_void();
}

std::optional<HazardEstimator::Estimate>
HazardEstimator::estimate_locked(const NodeId &node_id,
                                 int64_t decision_ns) const {
  const auto found = state_.find(node_id);
  if (decision_ns <= 0 || found == state_.end() ||
      found->second.observed_since_ns > decision_ns)
    return std::nullopt;
  const auto &history = found->second;
  const auto cutoff_ns =
      decision_ns > window_ns_ ? decision_ns - window_ns_ : 0;
  const auto first =
      std::upper_bound(history.events.begin(), history.events.end(), cutoff_ns);
  const auto past_end = std::upper_bound(history.events.begin(),
                                         history.events.end(), decision_ns);
  const auto event_count =
      static_cast<uint64_t>(std::distance(first, past_end));
  const auto exposure_start = std::max(cutoff_ns, history.observed_since_ns);
  const double measured_exposure =
      static_cast<double>(decision_ns - exposure_start) * 1e-9;
  const double exposure_s =
      measured_exposure > 0.0 ? measured_exposure : window_s_;
  NodeHazard hazard;
  hazard.lambda = static_cast<double>(event_count) / exposure_s;
  hazard.event_count = event_count;
  hazard.confidence_available = measured_exposure > 0.0;
  if (past_end != history.events.begin())
    hazard.last_event_ns = *std::prev(past_end);
  const auto sample_end =
      std::upper_bound(history.samples.begin(), history.samples.end(),
                       decision_ns, [](int64_t stamp, const Sample &sample) {
                         return stamp < sample.timestamp_ns;
                       });
  if (sample_end != history.samples.begin()) {
    const auto &sample = *std::prev(sample_end);
    hazard.lambda = sample.lambda;
    hazard.last_sample_ns = sample.timestamp_ns;
    hazard.confidence_available = false;
  }
  return Estimate{hazard, exposure_s};
}

std::optional<double> HazardEstimator::estimate_at(const NodeId &node_id,
                                                   int64_t decision_ns) const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  const auto result = estimate_locked(node_id, decision_ns);
  if (!result)
    return std::nullopt;
  return result->hazard.lambda;
}
std::optional<NodeHazard>
HazardEstimator::summary_at(const NodeId &node_id, int64_t decision_ns) const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  const auto result = estimate_locked(node_id, decision_ns);
  return result ? std::optional<NodeHazard>{result->hazard} : std::nullopt;
}

std::optional<double>
HazardEstimator::confidence_upper_bound(const NodeId &node_id,
                                        int64_t decision_ns) const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  const auto result = estimate_locked(node_id, decision_ns);
  if (!result || !result->hazard.confidence_available)
    return std::nullopt;
  const double count = static_cast<double>(result->hazard.event_count);
  const double square = confidence_z_ * confidence_z_;
  return (count + square / 2.0 +
          confidence_z_ * std::sqrt(count + square / 4.0)) /
         result->exposure_s;
}

double HazardEstimator::upper_bound(const NodeId &node_id,
                                    int64_t decision_ns) const {
  std::shared_lock<std::shared_mutex> lock(mu_);
  const auto result = estimate_locked(node_id, decision_ns);
  if (!result)
    return 0.0;
  if (!result->hazard.confidence_available)
    return result->hazard.lambda;
  const double count = static_cast<double>(result->hazard.event_count);
  const double square = confidence_z_ * confidence_z_;
  return (count + square / 2.0 +
          confidence_z_ * std::sqrt(count + square / 4.0)) /
         result->exposure_s;
}

std::optional<double> HazardEstimator::estimate(const NodeId &node_id) const {
  return estimate_at(node_id, now_ns());
}

double HazardEstimator::lambda(const NodeId &node_id) const {
  return estimate(node_id).value_or(0.0);
}

double HazardEstimator::survival_prob(const NodeId &node_id,
                                      double horizon_s) const {
  if (!std::isfinite(horizon_s) || horizon_s < 0.0)
    throw std::invalid_argument(
        "Hazard survival horizon must be finite and nonnegative");
  return std::exp(-lambda(node_id) * horizon_s);
}

std::unordered_map<NodeId, NodeHazard> HazardEstimator::snapshot() const {
  const auto decision_ns = now_ns();
  std::shared_lock<std::shared_mutex> lock(mu_);
  std::unordered_map<NodeId, NodeHazard> result;
  for (const auto &entry : state_) {
    const auto estimate = estimate_locked(entry.first, decision_ns);
    if (estimate)
      result.emplace(entry.first, estimate->hazard);
  }
  return result;
}

void HazardEstimator::prune(double max_age_s) {
  const auto max_age_ns = validated_window_ns(max_age_s);
  const auto current_ns = now_ns();
  const auto threshold_ns =
      current_ns > max_age_ns ? current_ns - max_age_ns : 0;
  std::unique_lock<std::shared_mutex> lock(mu_);
  for (auto entry = state_.begin(); entry != state_.end();) {
    const auto &history = entry->second;
    auto last_ns =
        std::max(history.observed_since_ns, history.last_observation_ns);
    if (!history.events.empty())
      last_ns = std::max(last_ns, history.events.back());
    if (!history.samples.empty())
      last_ns = std::max(last_ns, history.samples.back().timestamp_ns);
    if (last_ns < threshold_ns)
      entry = state_.erase(entry);
    else
      ++entry;
  }
}

}
}
