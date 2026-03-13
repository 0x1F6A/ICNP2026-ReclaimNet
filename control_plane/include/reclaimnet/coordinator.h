#pragma once
/// @file coordinator.h
/// @brief Core coordinator logic: epoch loop, P1/P2/P3 orchestration.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/config.h"
#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/p2_selector.h"
#include "reclaimnet/p3_admission.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/job_registry.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/persistence.h"

#include <atomic>
#include <thread>
#include <memory>
#include <functional>

namespace reclaimnet {
namespace control {

/// Migration command issued to a node agent.
struct MigrationCommand {
  JobId    job_id;
  NodeId   dst_node;
  double   allocated_bps;
  double   deadline_s;        ///< absolute POSIX seconds
};

/// Checkpoint schedule command issued to a node agent.
struct CheckpointCommand {
  JobId  job_id;
  double interval_s;
  double allocated_bps;
};

/// Top-level coordinator.  Owns all subsystems.
class Coordinator {
public:
  explicit Coordinator(CoordinatorConfig cfg);
  ~Coordinator();

  /// Blocking startup: loads topology, opens DB, begins epoch thread.
  VoidResult start();

  /// Graceful shutdown.
  void stop();

  // ── Event handlers called by the gRPC service layer ──────────────────────

  /// Process a node heartbeat; update registry and hazard estimator.
  void on_heartbeat(const NodeId& node_id, const NodeTelemetry& tel);

  /// Process a reclaim signal; trigger P2 destination selection + P3 admission.
  Result<MigrationCommand> on_reclaim_signal(
      const NodeId& src_node,
      const JobId& job_id,
      double deadline_s,
      ReclaimType type);

  /// Update migration progress; release admission on completion.
  void on_migration_progress(const JobId& job_id, uint64_t bytes_done, bool completed);

  /// Register a new node; persist it.
  VoidResult register_node(const NodeRecord& rec);

  /// Register a new job.
  VoidResult register_job(const JobRecord& rec);

  // ── Accessors ─────────────────────────────────────────────────────────────

  const JobRegistry&    jobs()    const { return job_reg_; }
  const NodeRegistry&   nodes()   const { return node_reg_; }
  const HazardEstimator& hazard() const { return hazard_; }

  bool running() const noexcept { return running_.load(); }

private:
  CoordinatorConfig cfg_;

  // Sub-systems.
  std::unique_ptr<TopologyGraph> topo_;
  HazardEstimator                hazard_;
  JobRegistry                    job_reg_;
  NodeRegistry                   node_reg_;
  std::unique_ptr<P1Allocator>   p1_;
  std::unique_ptr<P2Selector>    p2_;
  std::unique_ptr<P3Admission>   p3_;
  std::unique_ptr<PersistenceStore> store_;

  // Epoch loop.
  std::atomic<bool> running_{false};
  std::thread       epoch_thread_;
  void              run_epoch_loop();
  void              run_one_epoch();
};

}  // namespace control
}  // namespace reclaimnet
