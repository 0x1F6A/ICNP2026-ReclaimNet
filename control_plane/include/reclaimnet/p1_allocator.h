#pragma once

#include "reclaimnet/types.h"

#include <string>
#include <vector>

namespace reclaimnet {
namespace control {

struct P1JobInput {
  JobId job_id;
  double lambda_i = 0.0;
  uint64_t checkpoint_size_b = 0;
  double write_cap_bps = 0.0;
  double checkpoint_beta = 0.2;
  double loss_budget_s = 18000.0;
};

struct P1Schedule {
  JobId job_id;
  double delta_t_s = 0.0;
  double b_i_bps = 0.0;
  double write_rate_limit_bps = 0.0;
  double raw_delta_t_s = 0.0;
  double lower_bound_s = 0.0;
  double upper_bound_s = 0.0;
  bool feasible = false;
  std::string reason;
};

struct P1EpochResult {
  std::vector<P1Schedule> schedules;
  double total_bps_used = 0.0;
  int water_fill_iters = 0;
};

class P1Allocator {
public:
  explicit P1Allocator(double B_ckpt_bps);

  P1EpochResult compute(const std::vector<P1JobInput> &jobs) const;

  static double optimal_interval_s(double lambda_e, double C,
                                   double B_eff) noexcept;

  double budget_bps() const noexcept { return B_ckpt_bps_; }
  void set_budget_bps(double bps) noexcept { B_ckpt_bps_ = bps; }

private:
  double B_ckpt_bps_;

  std::vector<double> water_fill(const std::vector<P1JobInput> &jobs,
                                 int *iters_out) const;
};

}
}
