#pragma once
/// @file grpc_service.h
/// @brief gRPC service implementations: CoordinatorService + NodeAgentService stubs.

#include "reclaimnet/coordinator.h"

// gRPC generated stubs – conditionally included.
#if __has_include(<reclaimnet.grpc.pb.h>)
#  include <reclaimnet.grpc.pb.h>
#  define HAVE_PROTO 1
#else
#  define HAVE_PROTO 0
#endif

#include <memory>
#include <string>

namespace reclaimnet {
namespace control {

#if HAVE_PROTO

/// Implements reclaimnet.CoordinatorService for both coordinator and agent callers.
class CoordinatorServiceImpl final : public reclaimnet::proto::CoordinatorService::Service {
public:
  explicit CoordinatorServiceImpl(Coordinator& coord);

  grpc::Status RegisterNode(grpc::ServerContext* ctx,
                             const proto::RegisterNodeRequest* req,
                             proto::RegisterNodeResponse* resp) override;

  grpc::Status RegisterJob(grpc::ServerContext* ctx,
                            const proto::RegisterJobRequest* req,
                            proto::RegisterJobResponse* resp) override;

  grpc::Status Heartbeat(grpc::ServerContext* ctx,
                          const proto::HeartbeatRequest* req,
                          proto::HeartbeatResponse* resp) override;

  grpc::Status ReclaimSignal(grpc::ServerContext* ctx,
                              const proto::ReclaimSignalRequest* req,
                              proto::ReclaimSignalResponse* resp) override;

  grpc::Status MigrationProgress(grpc::ServerContext* ctx,
                                  const proto::MigrationProgressRequest* req,
                                  proto::MigrationProgressResponse* resp) override;

  grpc::Status GetTopology(grpc::ServerContext* ctx,
                            const proto::GetTopologyRequest* req,
                            proto::GetTopologyResponse* resp) override;

private:
  Coordinator& coord_;
};

#else
/// Placeholder when proto headers are not present (build-time only).
class CoordinatorServiceImpl {
public:
  explicit CoordinatorServiceImpl(Coordinator& coord) : coord_(coord) {}
private:
  Coordinator& coord_;
};
#endif  // HAVE_PROTO

/// Build and run a gRPC server hosting CoordinatorServiceImpl.
/// Blocks until stop() is called or SIGTERM received.
VoidResult run_grpc_server(Coordinator& coord,
                            const std::string& listen_addr,
                            bool mtls_enabled,
                            const std::string& cert, const std::string& key,
                            const std::string& ca);

}  // namespace control
}  // namespace reclaimnet
