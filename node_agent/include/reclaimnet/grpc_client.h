#pragma once

#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/result.h"
#include "reclaimnet/telemetry.h"
#include "reclaimnet/types.h"

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#if RECLAIMNET_HAVE_PROTO
#include <reclaimnet.grpc.pb.h>
#endif

namespace grpc {
class Channel;
}

namespace reclaimnet {
namespace agent {

struct CheckpointPlan {
  JobId job_id;
  uint64_t interval_ns;
  uint64_t allocated_bps;
  uint64_t write_rate_limit_bps;
  bool feasible;
};

class CoordinatorClient {
public:
  CoordinatorClient(std::string coordinator_addr, bool mtls_enabled,
                    const std::string &cert_path, const std::string &key_path,
                    const std::string &ca_path);

  VoidResult
  register_node(const std::string &node_id, const std::string &address,
                const std::string &building, const std::string &switch_id,
                uint64_t total_vram_bytes, uint64_t link_capacity_bps,
                uint32_t gpu_count = 1, double cuda_capability = 0,
                uint16_t migration_port = 7600);

  VoidResult register_job(const std::string &job_id, const std::string &node_id,
                          uint64_t vram_bytes, uint64_t checkpoint_size_bytes,
                          const std::string &user, const std::string &tag,
                          double write_cap_bps = 0.0,
                          std::optional<double> loss_budget_s = std::nullopt,
                          double lambda_e = 0.0);

  VoidResult heartbeat(const std::string &node_id, const LocalTelemetry &tel);

  Result<ReclaimResponse> reclaim_signal(const ReclaimNotification &notif,
                                         const std::string &node_id);

  VoidResult migration_progress(const JobId &job_id, uint64_t bytes_transferred,
                                bool completed);

  bool connected() const noexcept { return connected_; }

  std::vector<CheckpointPlan> checkpoint_plans() const;
#if RECLAIMNET_HAVE_PROTO
  VoidResult register_runtime(const proto::RuntimeRegistration &registration,
                              const NodeId &node);
  Result<proto::BatchMigrationResponse>
  migration_batch(const proto::BatchMigrationRequest &request);
  Result<proto::ReclaimSignalResponse>
  migration_decision(const ReclaimNotification &request, const NodeId &node);
  Result<proto::CheckpointInterval>
  begin_checkpoint(const proto::CheckpointRequest &request);
  VoidResult commit_checkpoint(const proto::CheckpointInfo &info);
  Result<proto::RateUpdate>
  prepare_final_checkpoint(const proto::FinalCheckpointRequest &request);
  VoidResult source_stopped(const proto::SourceStopResult &result);
  VoidResult report_telemetry(const proto::TelemetryReport &report);
  VoidResult recovery_complete(const proto::RecoveryResult &result);
  VoidResult migration_progress(const proto::MigrationProgressRequest &request);
  proto::HeartbeatResponse control_response() const;
  void authorize(grpc::ClientContext &context) const;
#endif

private:
  std::string addr_;
  bool connected_ = false;
  std::string initialization_error_;
  mutable std::mutex plans_mutex_;
  std::vector<CheckpointPlan> checkpoint_plans_;
  std::string access_token_;
#if RECLAIMNET_HAVE_PROTO
  proto::HeartbeatResponse control_response_;
#endif

  std::shared_ptr<grpc::Channel> channel_;

  VoidResult ensure_connected() const;
};

}
}
