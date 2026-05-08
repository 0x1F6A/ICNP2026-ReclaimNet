#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace reclaimnet {
namespace control {

enum class MigrationTrafficClass {
  EMERGENCY_METADATA,
  PLANNED_MIGRATION,
  PERIODIC_CHECKPOINT
};

struct MigrationEntry {
  JobId job_id;
  NodeId src;
  NodeId dst;
  uint64_t data_remaining_bytes = 0;
  double deadline_s = 0.0;
  double allocated_bps = 0.0;
  int64_t start_ns = 0;
  bool emergency = false;
  double restore_time_s = 0.0;
  int64_t deadline_ns = 0;
  uint64_t data_total_bytes = 0;
  uint64_t bytes_done = 0;
  std::vector<NodeId> path_nodes;
  std::string migration_id;
  uint32_t rank = 0;
  uint32_t gang_size = 1;
  double flow_cap_bps = 0.0;
  double minimum_useful_transfer_s = 1.0;
  int64_t eligible_start_ns = 0;
  uint64_t vram_required_bytes = 0;
  uint32_t gpu_count_required = 0;
  MigrationTrafficClass traffic_class =
      MigrationTrafficClass::PLANNED_MIGRATION;
  double minimum_required_bps = 0.0;
  uint64_t allocation_version = 0;
  bool restore_required = true;
};

struct DegradedMigration {
  MigrationEntry entry;
  std::string reason;
  bool recovery_required = true;
};

struct BatchAdmissionResult {
  std::vector<MigrationEntry> admitted;
  std::vector<DegradedMigration> degraded;
  std::vector<MigrationEntry> rate_updates;
};

struct PeriodicCheckpointDemand {
  JobId job_id;
  NodeId src;
  NodeId dst;
  uint64_t checkpoint_size_bytes = 0;
  double interval_s = 0.0;
  double write_rate_limit_bps = 0.0;
  double loss_budget_s = 18000.0;
  std::vector<NodeId> path_nodes;
  std::string demand_id;
  uint32_t rank = 0;
  uint32_t gang_size = 1;
  uint64_t bytes_done = 0;
};

struct LinkState {
  NodeId src;
  NodeId dst;
  double capacity_bps = 0.0;
  double allocated_bps = 0.0;
  size_t active_count = 0;
};

class P3Admission {
public:
  explicit P3Admission(double migration_share = 0.70,
                       double research_min_bps = 50'000'000.0);
  void add_link(const NodeId &src, const NodeId &dst, double cap_bps);
  void set_destination_resources(const NodeId &dst, uint64_t free_vram_bytes,
                                 uint32_t free_gpu_count);
  Result<MigrationEntry> admit(const MigrationEntry &entry);
  BatchAdmissionResult admit_batch(const std::vector<MigrationEntry> &entries,
                                   int64_t now_ns);
  BatchAdmissionResult
  schedule_checkpoints(const std::vector<PeriodicCheckpointDemand> &demands,
                       int64_t now_ns);
  std::optional<MigrationEntry>
  get_reservation(const JobId &job_id, uint32_t rank = 0,
                  const std::string &migration_id = "") const;
  void release(const JobId &job_id);
  void release_migration(const std::string &migration_id);
  void update_progress(const JobId &job_id, uint64_t bytes_done);
  void update_rank_progress(const std::string &migration_id, uint32_t rank,
                            uint64_t bytes_done);
  VoidResult update_payload(const std::string &migration_id, uint32_t rank,
                            uint64_t bytes);
  std::vector<MigrationEntry> expired_migrations(int64_t now_ns) const;
  std::vector<MigrationEntry> active_migrations() const;
  std::vector<LinkState> link_states() const;
  std::vector<DegradedMigration> degraded_migrations() const;
  void emergency_drain(const NodeId &src);
  size_t active_count() const;
  double residual_bps(const NodeId &src, const NodeId &dst) const;

private:
  struct DestinationState {
    uint64_t free_vram_bytes = 0;
    uint32_t free_gpu_count = 0;
    uint64_t allocated_vram_bytes = 0;
    uint32_t allocated_gpu_count = 0;
  };
  double migration_share_;
  double research_min_bps_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, MigrationEntry> migrations_;
  std::unordered_map<std::string, LinkState> links_;
  std::unordered_map<NodeId, DestinationState> destinations_;
  std::vector<DegradedMigration> degraded_;
  uint64_t next_version_ = 1;
  static std::string link_key(const NodeId &src, const NodeId &dst);
  static std::string migration_key(const MigrationEntry &entry);
  double available_bps(const std::string &key) const;
  Result<MigrationEntry> normalize(const MigrationEntry &entry,
                                   int64_t decision_ns, bool strict_now) const;
  bool same_request(const MigrationEntry &first,
                    const MigrationEntry &second) const;
  VoidResult reserve(const MigrationEntry &entry);
  void unreserve(const MigrationEntry &entry);
  void rebuild_link_usage();
  std::vector<MigrationEntry> rebalance(int64_t now_ns);
};

}
}
