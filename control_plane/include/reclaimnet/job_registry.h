#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace reclaimnet {
namespace control {

struct JobRecord {
  JobId job_id;
  NodeId node_id;
  JobState state = JobState::UNKNOWN;
  int64_t submit_time_ns = 0;
  int64_t last_update_ns = 0;
  uint64_t vram_bytes = 0;
  double lambda_i = 0;
  uint64_t checkpoint_size = 0;
  double progress = 0;
  std::string user;
  std::string workload_tag;
  NodeId migration_dst;
  double checkpoint_interval_s = 0;
  double checkpoint_allocated_bps = 0;
  double checkpoint_write_rate_bps = 0;
  double write_cap_bps = 0;
  double loss_budget_s = 0;
  bool checkpoint_constraints_feasible = false;
  double cuda_capability_required = 0.0;
  uint32_t gpu_count_required = 1;
  double remaining_runtime_s = 3600.0;
  std::vector<NodeId> rank_nodes;
  uint32_t rank_count = 1;
  std::vector<uint64_t> rank_checkpoint_sizes;
};

class JobRegistry {
public:
  JobRegistry() = default;

  VoidResult add(const JobRecord &rec);

  VoidResult update(const JobId &id, std::function<void(JobRecord &)> mutator);

  std::optional<JobRecord> get(const JobId &id) const;

  void remove(const JobId &id);
  void clear();

  std::vector<JobRecord> by_state(JobState s) const;

  std::vector<JobRecord> by_node(const NodeId &n) const;

  size_t size() const;

  std::vector<JobRecord> all() const;

private:
  mutable std::mutex mu_;
  std::unordered_map<JobId, JobRecord> jobs_;
};

}
}
