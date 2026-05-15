#pragma once

#include "reclaimnet/coordinator.h"

#if defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
#include <reclaimnet.grpc.pb.h>
#endif

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace reclaimnet {
namespace control {

#if defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO

class CoordinatorServiceImpl final
    : public reclaimnet::proto::CoordinatorService::Service {
public:
  explicit CoordinatorServiceImpl(Coordinator &coord);

  grpc::Status Register(grpc::ServerContext *ctx,
                        const proto::RegisterRequest *req,
                        proto::RegisterResponse *resp) override;

  grpc::Status ReportTelemetry(grpc::ServerContext *ctx,
                               const proto::TelemetryReport *req,
                               proto::Empty *resp) override;

  grpc::Status ReportReclaim(grpc::ServerContext *ctx,
                             const proto::ReclaimEvent *req,
                             proto::Empty *resp) override;

  grpc::Status RequestMigration(grpc::ServerContext *ctx,
                                const proto::MigrationRequest *req,
                                proto::MigrationDecision *resp) override;

  grpc::Status UpdateRateBudget(grpc::ServerContext *ctx,
                                const proto::RateLimit *req,
                                proto::AdmissionResult *resp) override;

  grpc::Status GetPrometheusSnapshot(grpc::ServerContext *ctx,
                                     const proto::Empty *req,
                                     proto::PrometheusSnapshot *resp) override;

  grpc::Status RegisterNode(grpc::ServerContext *ctx,
                            const proto::RegisterNodeRequest *req,
                            proto::RegisterNodeResponse *resp) override;

  grpc::Status RegisterJob(grpc::ServerContext *ctx,
                           const proto::RegisterJobRequest *req,
                           proto::RegisterJobResponse *resp) override;

  grpc::Status Heartbeat(grpc::ServerContext *ctx,
                         const proto::HeartbeatRequest *req,
                         proto::HeartbeatResponse *resp) override;

  grpc::Status ReclaimSignal(grpc::ServerContext *ctx,
                             const proto::ReclaimSignalRequest *req,
                             proto::ReclaimSignalResponse *resp) override;

  grpc::Status
  MigrationProgress(grpc::ServerContext *ctx,
                    const proto::MigrationProgressRequest *req,
                    proto::MigrationProgressResponse *resp) override;

  grpc::Status GetTopology(grpc::ServerContext *ctx,
                           const proto::GetTopologyRequest *req,
                           proto::GetTopologyResponse *resp) override;

  grpc::Status PrepareFinalCheckpoint(grpc::ServerContext *,
                                      const proto::FinalCheckpointRequest *,
                                      proto::RateUpdate *) override;
  grpc::Status SourceStopped(grpc::ServerContext *,
                             const proto::SourceStopResult *,
                             proto::Empty *) override;
  grpc::Status BeginCheckpoint(grpc::ServerContext *,
                               const proto::CheckpointRequest *,
                               proto::CheckpointInterval *) override;
  grpc::Status CommitCheckpoint(grpc::ServerContext *,
                                const proto::CheckpointInfo *,
                                proto::Empty *) override;
  grpc::Status RecoveryComplete(grpc::ServerContext *,
                                const proto::RecoveryResult *,
                                proto::Empty *) override;
  grpc::Status MigrationBatch(grpc::ServerContext *,
                              const proto::BatchMigrationRequest *,
                              proto::BatchMigrationResponse *) override;

private:
  Coordinator &coord_;
  std::string jwt_public_key_, jwt_private_key_;
  mutable std::mutex token_mutex_;
  mutable std::unordered_map<NodeId, std::pair<int64_t, std::string>> tokens_;
  grpc::Status authenticate(grpc::ServerContext *, const NodeId & = {});
  std::string token(const NodeId &) const;
};

#else

class CoordinatorServiceImpl {
public:
  explicit CoordinatorServiceImpl(Coordinator &coord) : coord_(coord) {}

private:
  Coordinator &coord_;
};
#endif

VoidResult run_grpc_server(Coordinator &coord, const std::string &listen_addr,
                           bool mtls_enabled, const std::string &cert,
                           const std::string &key, const std::string &ca);

}
}
