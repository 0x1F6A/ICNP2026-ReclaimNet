#pragma once



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


struct MigrationCommand {
  JobId    job_id;
  NodeId   dst_node;
  double   allocated_bps;
  double   deadline_s;
};


struct CheckpointCommand {
  JobId  job_id;
  double interval_s;
  double allocated_bps;
};


class Coordinator {
public:
  explicit Coordinator(CoordinatorConfig cfg);
  ~Coordinator();


  VoidResult start();


  void stop();




  void on_heartbeat(const NodeId& node_id, const NodeTelemetry& tel);


  Result<MigrationCommand> on_reclaim_signal(
      const NodeId& src_node,
      const JobId& job_id,
      double deadline_s,
      ReclaimType type);


  void on_migration_progress(const JobId& job_id, uint64_t bytes_done, bool completed);


  VoidResult register_node(const NodeRecord& rec);


  VoidResult register_job(const JobRecord& rec);



  const JobRegistry&    jobs()    const { return job_reg_; }
  const NodeRegistry&   nodes()   const { return node_reg_; }
  const HazardEstimator& hazard() const { return hazard_; }

  bool running() const noexcept { return running_.load(); }

private:
  CoordinatorConfig cfg_;


  std::unique_ptr<TopologyGraph> topo_;
  HazardEstimator                hazard_;
  JobRegistry                    job_reg_;
  NodeRegistry                   node_reg_;
  std::unique_ptr<P1Allocator>   p1_;
  std::unique_ptr<P2Selector>    p2_;
  std::unique_ptr<P3Admission>   p3_;
  std::unique_ptr<PersistenceStore> store_;


  std::atomic<bool> running_{false};
  std::thread       epoch_thread_;
  void              run_epoch_loop();
  void              run_one_epoch();
};

}
}
