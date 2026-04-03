#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <optional>
#include <functional>

namespace reclaimnet {
namespace control {


struct JobRecord {
  JobId       job_id;
  NodeId      node_id;
  JobState    state;
  int64_t     submit_time_ns;
  int64_t     last_update_ns;
  uint64_t    vram_bytes;
  double      lambda_i;
  uint64_t    checkpoint_size;
  double      progress;
  std::string user;
  std::string workload_tag;
  NodeId      migration_dst;
};


class JobRegistry {
public:
  JobRegistry() = default;


  VoidResult add(const JobRecord& rec);


  VoidResult update(const JobId& id, std::function<void(JobRecord&)> mutator);


  std::optional<JobRecord> get(const JobId& id) const;


  void remove(const JobId& id);


  std::vector<JobRecord> by_state(JobState s) const;


  std::vector<JobRecord> by_node(const NodeId& n) const;


  size_t size() const;


  std::vector<JobRecord> all() const;

private:
  mutable std::mutex mu_;
  std::unordered_map<JobId, JobRecord> jobs_;
};

}
}
