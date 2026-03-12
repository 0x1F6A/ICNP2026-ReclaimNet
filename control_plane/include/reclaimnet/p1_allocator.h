#pragma once
/// @file p1_allocator.h
/// @brief P1 – Reclaim-Aware Checkpoint Scheduling.
///
/// Computes:
///   - Optimal checkpoint interval:  Δt* = sqrt(2C / (λ_e · B_eff))
///   - Bandwidth share per job:      b_i ∝ (λ_i · C_i)^(1/3)  (water-filling)

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/config.h"

#include <unordered_map>
#include <vector>
#include <string>

namespace reclaimnet {
namespace control {

/// Input for the P1 scheduling computation for one job.
struct P1JobInput {
  JobId    job_id;
  double   lambda_i;           ///< per-job hazard rate (events/sec)
  uint64_t checkpoint_size_b;  ///< C_i bytes (current checkpoint size)
  double   write_cap_bps;      ///< per-job write bandwidth cap
};

/// P1 output for one job.
struct P1Schedule {
  JobId    job_id;
  double   delta_t_s;          ///< optimal checkpoint interval (seconds)
  double   b_i_bps;            ///< allocated bandwidth share (bps)
};

/// P1 allocation result for a full epoch.
struct P1EpochResult {
  std::vector<P1Schedule> schedules;
  double                  total_bps_used;
  int                     water_fill_iters;
};

/// Stateless P1 allocator.  Thread-safe (no mutable state).
class P1Allocator {
public:
  /// @param B_ckpt_bps  Total checkpoint bandwidth budget (bytes/sec).
  explicit P1Allocator(double B_ckpt_bps);

  /// Compute schedules for all active jobs.
  ///
  /// Implements cube-root proportional allocation then iterative water-filling
  /// to respect per-job write_cap constraints.
  P1EpochResult compute(const std::vector<P1JobInput>& jobs) const;

  /// Compute just the optimal interval for a single job.
  /// @param lambda_e  aggregate hazard rate (events/sec) for the node
  /// @param C         checkpoint size in bytes
  /// @param B_eff     effective write bandwidth for this job (bps)
  static double optimal_interval_s(double lambda_e, double C, double B_eff) noexcept;

  double budget_bps() const noexcept { return B_ckpt_bps_; }
  void   set_budget_bps(double bps) noexcept { B_ckpt_bps_ = bps; }

private:
  double B_ckpt_bps_;

  /// One pass of proportional allocation.
  std::vector<double> proportional_alloc(const std::vector<P1JobInput>& jobs) const;

  /// Water-filling iteration that respects write_cap_bps constraints.
  std::vector<double> water_fill(
      const std::vector<P1JobInput>& jobs,
      std::vector<double> init_alloc,
      int* iters_out) const;
};

}  // namespace control
}  // namespace reclaimnet
