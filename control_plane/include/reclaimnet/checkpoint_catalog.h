#pragma once
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace reclaimnet::control {
struct ReplicaCheckpoint {
  JobId job_id;
  std::string checkpoint_id;
  uint32_t sequence = 0;
  uint64_t size_bytes = 0;
  int64_t completed_at_ns = 0;
  std::vector<NodeId> source_node_ids;
  NodeId replica_node_id;
  std::string path;
  std::string checksum_sha256;
  uint64_t node_incarnation = 0;
  uint32_t rank = 0;
  uint32_t gang_size = 1;
};
class CheckpointCatalog {
public:
  VoidResult commit(const ReplicaCheckpoint &checkpoint);
  void invalidate_node(const NodeId &node, int64_t timestamp_ns);
  std::vector<ReplicaCheckpoint> freshest_surviving(
      const JobId &job, const std::vector<NodeId> &available_nodes,
      const std::vector<NodeId> &excluded_nodes, int64_t decision_ns,
      const std::function<bool(const ReplicaCheckpoint &)> &eligible = {})
      const;
  std::vector<ReplicaCheckpoint> all() const;

private:
  mutable std::mutex mu_;
  std::unordered_map<JobId, std::vector<ReplicaCheckpoint>> replicas_;
  std::unordered_map<NodeId, int64_t> invalidated_at_;
};
}
