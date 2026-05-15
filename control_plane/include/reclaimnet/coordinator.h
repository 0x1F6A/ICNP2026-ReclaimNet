#pragma once
#include "reclaimnet/capacity_estimator.h"
#include "reclaimnet/checkpoint_catalog.h"
#include "reclaimnet/config.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/job_registry.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/p2_selector.h"
#include "reclaimnet/p3_admission.h"
#include "reclaimnet/persistence.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/trace_writer.h"
#include "reclaimnet/types.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>
#include <unordered_map>

namespace reclaimnet::control {
struct RankMigrationCommand {
  uint32_t rank = 0;
  NodeId source_node;
  NodeId destination_node;
  std::string destination_address;
  uint16_t destination_port = 7600;
  double allocated_bps = 0;
  uint64_t checkpoint_bytes = 0;
  int64_t eligible_start_ns = 0;
  double restore_time_s = 0;
  uint32_t checkpoint_sequence = 0;
  uint64_t allocation_version = 0;
};
struct MigrationCommand {
  JobId job_id;
  NodeId dst_node;
  double allocated_bps = 0;
  double deadline_s = 0;
  int64_t deadline_ns = 0;
  std::string destination_address;
  uint16_t destination_port = 7600;
  std::string migration_id;
  uint32_t rank = 0;
  uint32_t gang_size = 1;
  int64_t eligible_start_ns = 0;
  bool recovery = false;
  std::vector<ReplicaCheckpoint> recoveries;
  std::vector<RankMigrationCommand> ranks;
};
struct CheckpointCommand {
  JobId job_id;
  double interval_s = 0;
  double allocated_bps = 0;
  double write_rate_limit_bps = 0;
  bool feasible = false;
  NodeId replica_node_id;
  std::string replica_address;
  uint16_t replica_port = 7600;
  std::string checkpoint_id;
  uint32_t rank = 0;
  uint32_t gang_size = 1;
  int64_t eligible_start_ns = 0;
  std::string migration_id;
  uint32_t checkpoint_sequence = 0;
  int64_t deadline_ns = 0;
  uint64_t allocation_version = 0;
};
struct ReclaimRequest {
  NodeId source_node;
  JobId job_id;
  double notice_s = 0;
  ReclaimType type = ReclaimType::SCHEDULED;
  int64_t timestamp_ns = 0;
};
class Coordinator {
public:
  explicit Coordinator(CoordinatorConfig cfg);
  ~Coordinator();
  VoidResult start(bool background_epochs = true);
  void stop();
  const CoordinatorConfig &config() const { return cfg_; }
  void on_heartbeat(const NodeId &, const NodeTelemetry &);
  VoidResult on_hazard_sample(const NodeId &, double, int64_t);
  VoidResult on_bandwidth_sample(const NodeId &, const NodeId &, double,
                                 int64_t);
  VoidResult on_reclaim_event(const NodeId &, const JobId &, double,
                              ReclaimType, int64_t = 0);
  Result<MigrationCommand> on_reclaim_signal(const NodeId &, const JobId &,
                                             double, ReclaimType, int64_t = 0);
  std::vector<Result<MigrationCommand>>
  on_reclaim_batch(const std::vector<ReclaimRequest> &);
  VoidResult on_migration_progress(const JobId &, uint64_t, bool,
                                   const std::string & = "", uint32_t = 0,
                                   bool = false, int64_t = 0);
  Result<CheckpointCommand> begin_checkpoint(const NodeId &, const JobId &,
                                             uint64_t, int64_t, uint32_t = 0);
  Result<MigrationEntry> prepare_final_payload(const JobId &,
                                               const std::string &, uint32_t,
                                               uint64_t, int64_t);
  VoidResult commit_checkpoint(const ReplicaCheckpoint &,
                               const std::string & = "", bool = false);
  VoidResult recovery_complete(const NodeId &, const JobId &,
                               const std::string &, uint32_t, int64_t);
  std::vector<CheckpointCommand> checkpoint_commands(const NodeId &) const;
  std::vector<MigrationEntry> rate_updates(const NodeId &) const;
  std::vector<ReplicaCheckpoint> recovery_commands(const NodeId &) const;
  std::vector<std::pair<JobId, std::string>> start_jobs(const NodeId &) const;
  std::vector<std::pair<JobId, std::string>> stop_jobs(const NodeId &) const;
  VoidResult source_stopped(const NodeId &, const JobId &, const std::string &,
                            int64_t);
  std::vector<std::pair<JobId, std::string>>
  resume_commands(const NodeId &node) const {
    return start_jobs(node);
  }
  VoidResult export_flow(const std::string &);
  std::string runtime_spec(const JobId &) const;
  void run_one_epoch(int64_t = 0);
  VoidResult register_node(const NodeRecord &, uint16_t = 7600);
  VoidResult register_job(const JobRecord &, std::string = "");
  const JobRegistry &jobs() const { return job_reg_; }
  const NodeRegistry &nodes() const { return node_reg_; }
  const HazardEstimator &hazard() const { return hazard_; }
  bool running() const noexcept { return running_; }

private:
  CoordinatorConfig cfg_;
  std::unique_ptr<TopologyGraph> topo_;
  HazardEstimator hazard_;
  CapacityEstimator capacities_;
  CheckpointCatalog catalog_;
  JobRegistry job_reg_;
  NodeRegistry node_reg_;
  std::unique_ptr<P1Allocator> p1_;
  std::unique_ptr<P2Selector> p2_;
  std::unique_ptr<P3Admission> p3_;
  std::unique_ptr<PersistenceStore> store_;
  std::unique_ptr<trace::Writer> writer_;
  std::atomic<bool> running_{false};
  std::thread epoch_thread_;
  mutable std::mutex control_mu_;
  std::mutex epoch_wait_mu_;
  std::condition_variable epoch_cv_;
  std::unordered_map<NodeId, uint16_t> migration_ports_;
  std::unordered_map<JobId, std::string> runtime_specs_;
  std::unordered_map<JobId, std::vector<NodeId>> replica_nodes_;
  std::unordered_map<JobId, std::vector<NodeId>> migration_destinations_;
  std::unordered_map<JobId, std::string> migration_sessions_;
  std::unordered_map<JobId, std::vector<CheckpointCommand>> checkpoint_plans_;
  std::unordered_map<JobId, uint32_t> checkpoint_sequences_;
  std::unordered_map<JobId, std::vector<ReplicaCheckpoint>> recovery_queue_;
  std::unordered_map<std::string, std::set<uint32_t>> completed_ranks_;
  std::unordered_map<NodeId, std::vector<MigrationEntry>> pending_rates_;
  std::unordered_map<std::string, std::set<uint32_t>> restored_ranks_;
  std::unordered_map<std::string, std::set<uint32_t>> committed_ranks_;
  std::unordered_map<std::string, uint32_t> checkpoint_sequence_ids_;
  std::unordered_map<std::string, std::vector<uint64_t>> checkpoint_sizes_;
  std::unordered_map<NodeId, std::vector<std::pair<JobId, std::string>>>
      resume_queue_;
  struct RecoveryBarrier {
    std::string id;
    std::set<NodeId> waiting;
  };
  std::unordered_map<JobId, RecoveryBarrier> recovery_barriers_;
  std::unordered_map<NodeId, std::vector<std::pair<JobId, std::string>>>
      stop_queue_;
  std::unordered_map<std::string, std::set<NodeId>> stopped_sources_;
  std::set<std::tuple<NodeId, JobId, std::string, uint32_t>> recovery_acks_;
  std::unordered_map<NodeId, int64_t> pending_invalidations_;
  uint64_t next_migration_id_ = 1;
  void run_epoch_loop();
  void apply_batch_locked(const BatchAdmissionResult &, int64_t);
  Result<MigrationCommand>
  recovery_locked(const JobRecord &, const std::vector<NodeId> &, int64_t);
  Result<MigrationCommand> migration_command_locked(const JobRecord &,
                                                    int64_t) const;
  void trace_locked(const std::string &, const std::string &);
  void snapshot_nodes_locked();
  void persist_job_locked(const JobId &);
  void allocate_checkpoints_locked(int64_t);
  std::unordered_map<NodeId, std::pair<uint64_t, uint32_t>>
  destination_capacity_locked() const;
  VoidResult queue_resume_locked(const JobId &, const std::string &,
                                 const std::vector<NodeId> &);
};
}
