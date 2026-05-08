#pragma once
#include "reclaimnet/result.h"
#include <cstdint>
#include <mutex>
#include <string>

namespace reclaimnet::trace {
class Writer {
public:
  Writer(std::string directory, std::string run_id);
  VoidResult initialize(double p1_beta, double loss_budget_s,
                        double global_budget_bps, double p3_beta,
                        double research_min_bps, double control_epoch_s = 30,
                        double p2_alpha = 1, size_t p2_k_min = 2,
                        double p2_load_limit_pct = 80,
                        double minimum_useful_transfer_s = 1,
                        double capacity_lower_bound_factor = .8);
  VoidResult finalize();
  VoidResult append(const std::string &file, const std::string &json);
  VoidResult snapshot(const std::string &file, const std::string &json);
  const std::string &run_id() const { return run_id_; }

private:
  std::string directory_;
  std::string run_id_;
  std::mutex mu_;
};
}
