#pragma once
/// @file job_registry.h
/// @brief In-memory registry for active and completed GPU jobs.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <optional>
#include <functional>

namespace reclaimnet {
namespace control {

/// Full runtime record for one job.
struct JobRecord {
  JobId       job_id;
  NodeId      node_id;          ///< current host
  JobState    state;
  int64_t     submit_time_ns;
  int64_t     last_update_ns;
  uint64_t    vram_bytes;
  double      lambda_i;         ///< per-job reclaim rate (from hazard estimator)
  uint64_t    checkpoint_size;  ///< C_i in bytes
  double      progress;         ///< 0.0 – 1.0
  std::string user;
  std::string workload_tag;
  NodeId      migration_dst;    ///< set during migration
};

/// Thread-safe job registry.
class JobRegistry {
public:
  JobRegistry() = default;

  /// Register a new job.  Returns error if job_id already exists.
  VoidResult add(const JobRecord& rec);

  /// Update fields of an existing job.
  VoidResult update(const JobId& id, std::function<void(JobRecord&)> mutator);

  /// Look up a job by ID.
  std::optional<JobRecord> get(const JobId& id) const;

  /// Remove a job from the active registry.
  void remove(const JobId& id);

  /// All jobs in the given state.
  std::vector<JobRecord> by_state(JobState s) const;

  /// All jobs on a given node.
  std::vector<JobRecord> by_node(const NodeId& n) const;

  /// Total number of registered jobs.
  size_t size() const;

  /// Iterate over all jobs (read-only snapshot).
  std::vector<JobRecord> all() const;

private:
  mutable std::mutex mu_;
  std::unordered_map<JobId, JobRecord> jobs_;
};

}  // namespace control
}  // namespace reclaimnet
