/// @file hazard_estimator.cc
/// @brief NHPP hazard-rate estimator – EWMA over reclaim event stream.

#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <cmath>
#include <algorithm>

namespace reclaimnet {
namespace control {

// ── Constructor ───────────────────────────────────────────────────────────────

HazardEstimator::HazardEstimator(double decay_half_life_s)
  : decay_alpha_(std::log(2.0) / decay_half_life_s) {}

// ── record_event ──────────────────────────────────────────────────────────────

void HazardEstimator::record_event(const ReclaimEvent& ev) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = state_.find(ev.node_id);
  if (it == state_.end()) {
    // First event for this node – initialise with a single-event rate of 1/s.
    NodeHazard h{};
    h.lambda       = 1.0;
    h.last_event_ns = ev.timestamp_ns;
    h.event_count  = 1;
    state_.emplace(ev.node_id, h);
    LOG_DEBUG("HazardEstimator: first event for node %s, λ=1.0", ev.node_id.c_str());
    return;
  }

  NodeHazard& h = it->second;
  double dt_s   = static_cast<double>(ev.timestamp_ns - h.last_event_ns) / 1e9;
  if (dt_s <= 0.0) dt_s = 1e-6;  // clamp numerical noise

  // Instantaneous rate for this inter-arrival.
  double inst_lambda = 1.0 / dt_s;

  // EWMA update: α controls the memory.
  // alpha_per_event = exp(-decay_alpha_ * dt_s)
  double alpha = std::exp(-decay_alpha_ * dt_s);
  h.lambda       = alpha * h.lambda + (1.0 - alpha) * inst_lambda;
  h.last_event_ns = ev.timestamp_ns;
  h.event_count++;

  LOG_DEBUG("HazardEstimator: node %s event #%llu, dt=%.3fs, λ=%.4f",
            ev.node_id.c_str(), (unsigned long long)h.event_count, dt_s, h.lambda);
}

// ── lambda ────────────────────────────────────────────────────────────────────

double HazardEstimator::lambda(const NodeId& node_id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = state_.find(node_id);
  if (it == state_.end()) return 0.0;
  return it->second.lambda;
}

// ── survival_prob ─────────────────────────────────────────────────────────────

double HazardEstimator::survival_prob(const NodeId& node_id, double horizon_s) const {
  double lam = lambda(node_id);
  return std::exp(-lam * horizon_s);
}

// ── snapshot ──────────────────────────────────────────────────────────────────

std::unordered_map<NodeId, NodeHazard> HazardEstimator::snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  return state_;
}

// ── prune ─────────────────────────────────────────────────────────────────────

void HazardEstimator::prune(double max_age_s) {
  int64_t threshold_ns = now_ns() - static_cast<int64_t>(max_age_s * 1e9);
  std::lock_guard<std::mutex> lk(mu_);
  for (auto it = state_.begin(); it != state_.end(); ) {
    if (it->second.last_event_ns < threshold_ns)
      it = state_.erase(it);
    else
      ++it;
  }
}

}  // namespace control
}  // namespace reclaimnet
