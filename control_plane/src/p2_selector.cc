/// @file p2_selector.cc
/// @brief P2 volatility-aware migration destination selection.

#include "reclaimnet/p2_selector.h"
#include "reclaimnet/log.h"

#include <cmath>
#include <algorithm>
#include <limits>

namespace reclaimnet {
namespace control {

// ── topology_dist_score ───────────────────────────────────────────────────────
// Maps TopologyLevel to a normalised [0,1] distance score.

double P2Selector::topology_dist_score(TopologyLevel lvl) noexcept {
  switch (lvl) {
    case TopologyLevel::NVLink:    return 0.0;
    case TopologyLevel::PCIe:      return 0.1;
    case TopologyLevel::IntraRack: return 0.3;
    case TopologyLevel::InterRack: return 0.7;
    case TopologyLevel::WAN:       return 1.0;
    default:                       return 1.0;
  }
}

// ── Constructor ───────────────────────────────────────────────────────────────

P2Selector::P2Selector(double alpha, double beta, double gamma,
                       const TopologyGraph& topo,
                       const HazardEstimator& hazard,
                       const NodeRegistry& nodes)
  : alpha_(alpha), beta_(beta), gamma_(gamma),
    topo_(topo), hazard_(hazard), nodes_(nodes) {}

// ── evaluate ──────────────────────────────────────────────────────────────────

P2Candidate P2Selector::evaluate(
    const NodeRecord& candidate,
    const P2Query& q,
    double eff_alpha, double eff_beta, double eff_gamma,
    double horizon_s) const {

  P2Candidate c;
  c.node_id  = candidate.node_id;
  c.feasible = true;

  // Feasibility check: C_size/B + T_r > τ → infeasible
  if (candidate.link_capacity_bps > 0.0) {
    double T_transfer = static_cast<double>(q.checkpoint_size_b)
                        / candidate.link_capacity_bps;
    c.T_transfer_s = T_transfer;
    if (T_transfer + q.T_restore_s > q.reclaim_deadline_s) {
      c.feasible          = false;
      c.infeasible_reason = "transfer+restore exceeds deadline";
      c.composite_cost    = std::numeric_limits<double>::max();
      return c;
    }
  } else {
    c.feasible          = false;
    c.infeasible_reason = "zero link capacity";
    c.composite_cost    = std::numeric_limits<double>::max();
    return c;
  }

  // Sufficient VRAM check.
  if (candidate.telemetry.vram_free_bytes < q.checkpoint_size_b) {
    c.feasible          = false;
    c.infeasible_reason = "insufficient VRAM";
    c.composite_cost    = std::numeric_limits<double>::max();
    return c;
  }

  // Survival probability.
  c.survival_prob = hazard_.survival_prob(candidate.node_id, horizon_s);

  // Topology distance.
  TopologyLevel lvl = topo_.level_between(q.source_node, candidate.node_id);
  c.topology_dist   = topology_dist_score(lvl);

  // Composite cost: α·T + β·(1-S) + γ·D
  c.composite_cost = eff_alpha * c.T_transfer_s
                   + eff_beta  * (1.0 - c.survival_prob)
                   + eff_gamma * c.topology_dist;
  return c;
}

// ── select ────────────────────────────────────────────────────────────────────

P2Result P2Selector::select(const P2Query& q, double horizon_s) const {
  P2Result result;
  result.success = false;

  // Effective weights: use query overrides when non-zero.
  double ea = q.alpha > 0.0 ? q.alpha : alpha_;
  double eb = q.beta  > 0.0 ? q.beta  : beta_;
  double eg = q.gamma > 0.0 ? q.gamma : gamma_;

  auto all_nodes = nodes_.by_state(NodeState::ACTIVE);

  for (const NodeRecord& rec : all_nodes) {
    if (rec.node_id == q.source_node) continue;  // skip self
    P2Candidate c = evaluate(rec, q, ea, eb, eg, horizon_s);
    result.ranked.push_back(c);
  }

  // Sort: feasible first, then by composite_cost ascending.
  std::sort(result.ranked.begin(), result.ranked.end(),
    [](const P2Candidate& a, const P2Candidate& b) {
      if (a.feasible != b.feasible) return (int)a.feasible > (int)b.feasible;
      return a.composite_cost < b.composite_cost;
    });

  if (!result.ranked.empty() && result.ranked.front().feasible) {
    result.success  = true;
    result.selected = result.ranked.front().node_id;
    LOG_INFO("P2: job=%s selected dst=%s cost=%.4f",
             q.job_id.c_str(), result.selected.c_str(),
             result.ranked.front().composite_cost);
  } else {
    result.reason = "No feasible destination found for job " + q.job_id;
    LOG_WARN("P2: %s", result.reason.c_str());
  }
  return result;
}

}  // namespace control
}  // namespace reclaimnet
