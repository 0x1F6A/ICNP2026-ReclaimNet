#include "reclaimnet/checkpoint_catalog.h"
#include <algorithm>
#include <unordered_set>

namespace reclaimnet::control {
VoidResult CheckpointCatalog::commit(const ReplicaCheckpoint &checkpoint) {
  if (checkpoint.job_id.empty() || checkpoint.checkpoint_id.empty() ||
      checkpoint.replica_node_id.empty() || checkpoint.path.empty() ||
      checkpoint.checksum_sha256.size() != 64 ||
      checkpoint.completed_at_ns <= 0 || checkpoint.gang_size == 0 ||
      checkpoint.rank >= checkpoint.gang_size)
    return err_void("Checkpoint commit requires complete replica metadata");
  std::lock_guard<std::mutex> lock(mu_);
  auto &replicas = replicas_[checkpoint.job_id];
  auto existing =
      std::find_if(replicas.begin(), replicas.end(), [&](const auto &value) {
        return value.checkpoint_id == checkpoint.checkpoint_id &&
               value.replica_node_id == checkpoint.replica_node_id &&
               value.rank == checkpoint.rank;
      });
  if (existing == replicas.end())
    replicas.push_back(checkpoint);
  else if (existing->checksum_sha256 != checkpoint.checksum_sha256 ||
           existing->size_bytes != checkpoint.size_bytes)
    return err_void("Conflicting committed checkpoint identity");
  return ok_void();
}
void CheckpointCatalog::invalidate_node(const NodeId &node, int64_t stamp) {
  std::lock_guard<std::mutex> lock(mu_);
  invalidated_at_[node] = std::max(invalidated_at_[node], stamp);
}
std::vector<ReplicaCheckpoint> CheckpointCatalog::freshest_surviving(
    const JobId &job, const std::vector<NodeId> &available,
    const std::vector<NodeId> &excluded, int64_t decision_ns,
    const std::function<bool(const ReplicaCheckpoint &)> &eligible) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto found = replicas_.find(job);
  if (found == replicas_.end())
    return {};
  std::unordered_set<NodeId> online(available.begin(), available.end());
  std::unordered_set<NodeId> banned(excluded.begin(), excluded.end());
  std::vector<ReplicaCheckpoint> candidates;
  for (const auto &checkpoint : found->second) {
    if (eligible && !eligible(checkpoint))
      continue;
    auto invalidation = invalidated_at_.find(checkpoint.replica_node_id);
    if (!online.count(checkpoint.replica_node_id) ||
        banned.count(checkpoint.replica_node_id) ||
        checkpoint.completed_at_ns > decision_ns ||
        (invalidation != invalidated_at_.end() &&
         checkpoint.completed_at_ns <= invalidation->second &&
         invalidation->second <= decision_ns))
      continue;
    candidates.push_back(checkpoint);
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const auto &first, const auto &second) {
              if (first.sequence != second.sequence)
                return first.sequence > second.sequence;
              if (first.completed_at_ns != second.completed_at_ns)
                return first.completed_at_ns > second.completed_at_ns;
              return first.replica_node_id < second.replica_node_id;
            });
  for (const auto &newest : candidates) {
    std::vector<ReplicaCheckpoint> selected;
    std::unordered_set<uint32_t> ranks;
    std::unordered_set<NodeId> nodes;
    for (const auto &checkpoint : candidates) {
      if (checkpoint.sequence == newest.sequence &&
          checkpoint.checkpoint_id == newest.checkpoint_id &&
          checkpoint.gang_size == newest.gang_size &&
          !ranks.count(checkpoint.rank) &&
          !nodes.count(checkpoint.replica_node_id)) {
        selected.push_back(checkpoint);
        ranks.insert(checkpoint.rank);
        nodes.insert(checkpoint.replica_node_id);
      }
    }
    if (selected.size() == newest.gang_size) {
      std::sort(selected.begin(), selected.end(),
                [](const auto &first, const auto &second) {
                  return first.rank < second.rank;
                });
      return selected;
    }
  }
  return {};
}
std::vector<ReplicaCheckpoint> CheckpointCatalog::all() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<ReplicaCheckpoint> result;
  for (const auto &[_, replicas] : replicas_)
    result.insert(result.end(), replicas.begin(), replicas.end());
  return result;
}
}
