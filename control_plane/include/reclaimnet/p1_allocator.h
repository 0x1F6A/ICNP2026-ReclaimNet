#pragma once







#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/config.h"

#include <unordered_map>
#include <vector>
#include <string>

namespace reclaimnet {
namespace control {


struct P1JobInput {
  JobId    job_id;
  double   lambda_i;
  uint64_t checkpoint_size_b;
  double   write_cap_bps;
};


struct P1Schedule {
  JobId    job_id;
  double   delta_t_s;
  double   b_i_bps;
};


struct P1EpochResult {
  std::vector<P1Schedule> schedules;
  double                  total_bps_used;
  int                     water_fill_iters;
};


class P1Allocator {
public:

  explicit P1Allocator(double B_ckpt_bps);





  P1EpochResult compute(const std::vector<P1JobInput>& jobs) const;





  static double optimal_interval_s(double lambda_e, double C, double B_eff) noexcept;

  double budget_bps() const noexcept { return B_ckpt_bps_; }
  void   set_budget_bps(double bps) noexcept { B_ckpt_bps_ = bps; }

private:
  double B_ckpt_bps_;


  std::vector<double> proportional_alloc(const std::vector<P1JobInput>& jobs) const;


  std::vector<double> water_fill(
      const std::vector<P1JobInput>& jobs,
      std::vector<double> init_alloc,
      int* iters_out) const;
};

}
}
