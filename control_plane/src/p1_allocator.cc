


#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/log.h"

#include <cmath>
#include <algorithm>
#include <numeric>
#include <stdexcept>

namespace reclaimnet {
namespace control {



P1Allocator::P1Allocator(double B_ckpt_bps)
  : B_ckpt_bps_(B_ckpt_bps) {}




double P1Allocator::optimal_interval_s(double lambda_e, double C, double B_eff) noexcept {
  if (lambda_e <= 0.0 || B_eff <= 0.0 || C <= 0.0) return 300.0;
  return std::sqrt(2.0 * C / (lambda_e * B_eff));
}




std::vector<double> P1Allocator::proportional_alloc(
    const std::vector<P1JobInput>& jobs) const {
  std::vector<double> weights(jobs.size(), 0.0);
  double denom = 0.0;
  for (size_t i = 0; i < jobs.size(); ++i) {
    double w = std::cbrt(jobs[i].lambda_i * static_cast<double>(jobs[i].checkpoint_size_b));
    if (!std::isfinite(w) || w < 0.0) w = 0.0;
    weights[i] = w;
    denom += w;
  }
  std::vector<double> alloc(jobs.size(), 0.0);
  if (denom < 1e-15) {

    double share = B_ckpt_bps_ / (double)jobs.size();
    std::fill(alloc.begin(), alloc.end(), share);
  } else {
    for (size_t i = 0; i < jobs.size(); ++i)
      alloc[i] = B_ckpt_bps_ * weights[i] / denom;
  }
  return alloc;
}



std::vector<double> P1Allocator::water_fill(
    const std::vector<P1JobInput>& jobs,
    std::vector<double> alloc,
    int* iters_out) const {
  const int MAX_ITERS = 64;
  int iter = 0;
  double budget = B_ckpt_bps_;

  for (; iter < MAX_ITERS; ++iter) {

    std::vector<bool> capped(jobs.size(), false);
    double capped_sum = 0.0;
    bool any_new_cap = false;

    for (size_t i = 0; i < jobs.size(); ++i) {
      if (jobs[i].write_cap_bps > 0.0 && alloc[i] > jobs[i].write_cap_bps) {
        if (!capped[i]) any_new_cap = true;
        capped[i]   = true;
        alloc[i]    = jobs[i].write_cap_bps;
        capped_sum += jobs[i].write_cap_bps;
      }
    }
    if (!any_new_cap) break;


    double surplus    = budget - capped_sum;
    double free_denom = 0.0;
    for (size_t i = 0; i < jobs.size(); ++i) {
      if (!capped[i]) {
        free_denom += std::cbrt(jobs[i].lambda_i *
                                 static_cast<double>(jobs[i].checkpoint_size_b));
      }
    }
    if (free_denom < 1e-15) break;
    for (size_t i = 0; i < jobs.size(); ++i) {
      if (!capped[i]) {
        double w  = std::cbrt(jobs[i].lambda_i *
                               static_cast<double>(jobs[i].checkpoint_size_b));
        alloc[i]  = surplus * w / free_denom;
      }
    }
    budget = surplus + capped_sum;
  }
  if (iters_out) *iters_out = iter;
  return alloc;
}



P1EpochResult P1Allocator::compute(const std::vector<P1JobInput>& jobs) const {
  if (jobs.empty()) return {{}, 0.0, 0};

  auto alloc = proportional_alloc(jobs);
  int iters  = 0;
  alloc      = water_fill(jobs, std::move(alloc), &iters);

  double total = 0.0;
  std::vector<P1Schedule> schedules;
  schedules.reserve(jobs.size());
  for (size_t i = 0; i < jobs.size(); ++i) {
    double b_i   = alloc[i];
    double dt    = optimal_interval_s(jobs[i].lambda_i,
                                       static_cast<double>(jobs[i].checkpoint_size_b), b_i);
    schedules.push_back({jobs[i].job_id, dt, b_i});
    total += b_i;
    LOG_DEBUG("P1: job=%s Δt*=%.1fs b_i=%.0fbps",
              jobs[i].job_id.c_str(), dt, b_i);
  }
  return {schedules, total, iters};
}

}
}
