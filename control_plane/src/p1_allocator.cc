#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/log.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace reclaimnet {
namespace control {
namespace {

bool valid_input(const P1JobInput &job) {
  return std::isfinite(job.lambda_i) && job.lambda_i >= 0.0 &&
         job.checkpoint_size_b > 0 && std::isfinite(job.write_cap_bps) &&
         job.write_cap_bps >= 0.0 && std::isfinite(job.checkpoint_beta) &&
         job.checkpoint_beta > 0.0 && job.checkpoint_beta <= 1.0 &&
         std::isfinite(job.loss_budget_s) && job.loss_budget_s > 0.0 &&
         job.loss_budget_s <= std::numeric_limits<double>::max() / 2.0;
}

}

P1Allocator::P1Allocator(double B_ckpt_bps) : B_ckpt_bps_(B_ckpt_bps) {}

double P1Allocator::optimal_interval_s(double lambda_e, double C,
                                       double B_eff) noexcept {
  if (!std::isfinite(lambda_e) || !std::isfinite(C) || !std::isfinite(B_eff) ||
      lambda_e < 0.0 || C < 0.0 || B_eff < 0.0)
    return std::numeric_limits<double>::quiet_NaN();
  if (C == 0.0)
    return 0.0;
  if (lambda_e == 0.0 || B_eff == 0.0)
    return std::numeric_limits<double>::infinity();

  return static_cast<double>(
      std::sqrt(16.0L * C / (lambda_e * static_cast<long double>(B_eff))));
}

std::vector<double> P1Allocator::water_fill(const std::vector<P1JobInput> &jobs,
                                            int *iters_out) const {
  std::vector<double> allocation(jobs.size(), 0.0);
  std::vector<bool> fixed(jobs.size(), false);
  std::vector<long double> weights(jobs.size(), 0.0L);
  for (size_t i = 0; i < jobs.size(); ++i) {
    fixed[i] = !valid_input(jobs[i]);
    if (!fixed[i]) {
      weights[i] = std::cbrt(static_cast<long double>(jobs[i].lambda_i) *
                             jobs[i].checkpoint_size_b);
    }
  }

  long double remaining = B_ckpt_bps_;
  int iterations = 0;

  while (remaining > 0.0L) {
    long double weight_sum = 0.0L;
    size_t free_count = 0;
    for (size_t i = 0; i < jobs.size(); ++i) {
      if (!fixed[i]) {
        weight_sum += weights[i];
        ++free_count;
      }
    }
    if (free_count == 0)
      break;
    ++iterations;
    bool newly_capped = false;
    const long double round_budget = remaining;
    for (size_t i = 0; i < jobs.size(); ++i) {
      if (fixed[i])
        continue;
      const long double share = weight_sum > 0.0L
                                    ? round_budget * weights[i] / weight_sum
                                    : round_budget / free_count;
      if (jobs[i].write_cap_bps > 0.0 && share > jobs[i].write_cap_bps) {
        allocation[i] = jobs[i].write_cap_bps;
        remaining -= jobs[i].write_cap_bps;
        fixed[i] = true;
        newly_capped = true;
      }
    }
    if (newly_capped)
      continue;
    for (size_t i = 0; i < jobs.size(); ++i) {
      if (!fixed[i]) {
        allocation[i] = static_cast<double>(
            weight_sum > 0.0L ? remaining * weights[i] / weight_sum
                              : remaining / free_count);
      }
    }
    break;
  }
  if (iters_out)
    *iters_out = iterations;
  return allocation;
}

P1EpochResult P1Allocator::compute(const std::vector<P1JobInput> &jobs) const {
  P1EpochResult result;
  if (jobs.empty())
    return result;

  const bool valid_budget = std::isfinite(B_ckpt_bps_) && B_ckpt_bps_ > 0.0;
  std::vector<double> allocation(jobs.size(), 0.0);
  if (valid_budget)
    allocation = water_fill(jobs, &result.water_fill_iters);

  result.schedules.reserve(jobs.size());
  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto &job = jobs[i];
    P1Schedule schedule;
    schedule.job_id = job.job_id;
    schedule.b_i_bps = allocation[i];
    result.total_bps_used += schedule.b_i_bps;
    if (!valid_input(job)) {
      schedule.reason =
          "Invalid checkpoint size, hazard, cap, beta or loss budget";
    } else {
      schedule.upper_bound_s = 2.0 * job.loss_budget_s;
      schedule.raw_delta_t_s = optimal_interval_s(
          job.lambda_i, static_cast<double>(job.checkpoint_size_b),
          schedule.b_i_bps);
      schedule.write_rate_limit_bps = job.checkpoint_beta * schedule.b_i_bps;
      schedule.lower_bound_s =
          schedule.write_rate_limit_bps > 0.0
              ? 8.0 * static_cast<double>(job.checkpoint_size_b) /
                    schedule.write_rate_limit_bps
              : std::numeric_limits<double>::infinity();
      if (!valid_budget || schedule.b_i_bps <= 0.0) {
        schedule.reason = "No checkpoint bandwidth available";
      } else if (schedule.lower_bound_s > schedule.upper_bound_s) {
        schedule.reason = "Checkpoint duration exceeds the loss-budget bound";
      } else {
        schedule.delta_t_s =
            std::clamp(schedule.raw_delta_t_s, schedule.lower_bound_s,
                       schedule.upper_bound_s);
        schedule.feasible = true;
      }
    }
    LOG_DEBUG(
        "P1: job=%s interval=%.3fs slice=%.0fbps write=%.0fbps feasible=%d",
        job.job_id.c_str(), schedule.delta_t_s, schedule.b_i_bps,
        schedule.write_rate_limit_bps, static_cast<int>(schedule.feasible));
    result.schedules.push_back(std::move(schedule));
  }
  return result;
}

}
}
