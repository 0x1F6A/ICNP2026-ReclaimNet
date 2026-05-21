#pragma once
#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/config.h"
#include "reclaimnet/ebpf_loader.h"
#include "reclaimnet/gpu_isolation.h"
#include "reclaimnet/grpc_client.h"
#include "reclaimnet/migration_client.h"
#include "reclaimnet/telemetry.h"
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>
namespace reclaimnet::agent {
class AgentServiceHost;
class NodeAgent {
public:
  explicit NodeAgent(AgentConfig cfg,
                     std::shared_ptr<GpuBackend> gpu_backend = {});
  ~NodeAgent();
  VoidResult start();
  void stop();
  VoidResult handle_reclaim(const ReclaimNotification &notification);
  bool running() const noexcept { return running_.load(); }
  const AgentConfig &config() const { return cfg_; }
#if RECLAIMNET_HAVE_PROTO
  VoidResult register_runtime(const proto::RuntimeRegistration &registration,
                              bool upstream = true, bool allocate_gpu = true,
                              bool active = true);
  VoidResult prepare_migration(const proto::MigrationPreparation &preparation);
  VoidResult restore(const proto::RecoveryPlan &plan);
  VoidResult execute_migration(const proto::MigrationExecution &execution);
  void apply_checkpoint(const proto::CheckpointInterval &plan);
  VoidResult apply_kill(const proto::KillSwitchCommand &command);
#endif
private:
  AgentConfig cfg_;
  std::atomic<bool> running_{false}, cancel_{false};
  std::unique_ptr<RuntimeManager> runtime_;
  std::unique_ptr<CheckpointManager> checkpoints_;
  std::unique_ptr<MigrationClient> migration_;
  std::unique_ptr<MigrationServer> receiver_;
  std::unique_ptr<TelemetryCollector> telemetry_;
  std::unique_ptr<CoordinatorClient> coordinator_;
  std::unique_ptr<AgentServiceHost> service_;
  std::shared_ptr<GpuBackend> gpu_backend_;
  GpuIsolation gpu_;
  EbpfHandle ebpf_;
  int reclaim_map_ = -1;
  std::mutex mutex_;
  std::condition_variable wake_;
  std::thread control_thread_;
  std::vector<std::thread> workers_;
  std::set<JobId> in_flight_;
  std::set<JobId> active_jobs_;
  std::set<std::thread::id> finished_workers_;
  std::set<std::string> restored_checkpoints_;
  std::map<JobId, int64_t> last_checkpoint_;
  std::map<JobId, double> capture_durations_;
  std::set<std::string> started_barriers_;
  std::set<std::string> stopped_barriers_;
  std::set<std::string> accepted_migrations_;
  std::map<JobId, std::pair<uint32_t, int64_t>> pending_checkpoints_;
  std::map<std::string, double> rates_;
  std::map<std::string, uint64_t> rate_versions_;
  std::map<std::string, FiveTuple> active_flows_;
  std::map<std::string, bool> periodic_flows_;
  std::map<JobId, std::shared_ptr<std::atomic<bool>>> transfer_cancellations_;
#if RECLAIMNET_HAVE_PROTO
  std::map<JobId, proto::MigrationPreparation> preparations_;
  std::map<JobId, proto::CheckpointInterval> plans_;
  std::map<JobId, proto::RuntimeRegistration> registrations_;
  std::map<JobId, proto::MigrationExecution> pending_migrations_;
  VoidResult dispatch_reclaim(const ReclaimNotification &,
                              const proto::ReclaimSignalResponse &);
  VoidResult periodic_checkpoint(const proto::CheckpointInterval &plan);
  VoidResult transfer(const proto::RankMigration &rank, const JobId &job,
                      const std::string &migration_id, bool restoring,
                      uint32_t sequence = 0);
  std::shared_ptr<grpc::Channel>
  target_channel(const std::string &address) const;
  VoidResult prepare_target(const std::string &address,
                            const proto::MigrationPreparation &preparation);
  void configure_transfer(MigrationParams &params, const std::string &key,
                          bool periodic, uint64_t version);
#endif
  void control_loop();
  void launch(const JobId &id, std::function<VoidResult()> operation);
};
}
