#pragma once
#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/migration_client.h"
#include <functional>
#include <string>
namespace reclaimnet::agent {
struct ReclaimNotification {
  JobId job_id;
  double deadline_s = 0;
  ReclaimType type = ReclaimType::SCHEDULED;
  uint64_t vram_bytes = 0;
  int64_t event_timestamp_ns = 0;
};
struct ReclaimResponse {
  NodeId dst_node;
  uint16_t dst_port = 0;
  double allocated_bps = 0;
  double deadline_s = 0;
  int64_t deadline_ns = 0;
  std::optional<JobRuntimeSpec> destination_runtime;
  std::string dst_host;
};
using ReclaimSignalCb =
    std::function<Result<ReclaimResponse>(const ReclaimNotification &)>;
struct ReclaimCallbacks {
  std::function<bool(const ReclaimNotification &, uint64_t,
                     const ReclaimResponse &)>
      final_checkpoint_feasible;
  std::function<VoidResult(const ReclaimNotification &)> recover;
  std::function<VoidResult(const JobId &, uint64_t, bool)> progress;
  std::function<void(const JobId &, const Error &)> aborted;
  std::function<void(MigrationParams &)> configure_transfer;
};
class ReclaimHandler {
public:
  ReclaimHandler(CheckpointManager &checkpoints, MigrationClient &client,
                 ReclaimSignalCb signal, ReclaimCallbacks callbacks = {});
  VoidResult handle(const ReclaimNotification &notification);
  void cancel();

private:
  CheckpointManager &checkpoints_;
  MigrationClient &client_;
  ReclaimSignalCb signal_;
  ReclaimCallbacks callbacks_;
  std::atomic<bool> cancel_{false};
};
}
