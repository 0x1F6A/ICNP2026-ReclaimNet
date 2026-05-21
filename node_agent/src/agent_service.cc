#include "reclaimnet/agent_service.h"
#include "reclaimnet/agent.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/rpc_auth.h"
#include "reclaimnet/time.h"
#if RECLAIMNET_HAVE_PROTO
#include <grpcpp/grpcpp.h>
#include <reclaimnet.grpc.pb.h>
#endif
namespace reclaimnet::agent {
#if RECLAIMNET_HAVE_PROTO
struct AgentServiceHost::Impl : proto::NodeAgentService::Service {
  NodeAgent &agent;
  std::unique_ptr<grpc::Server> server;
  std::string public_key;
  int64_t started = now_ns();
  explicit Impl(NodeAgent &a) : agent(a) {}
  grpc::Status authenticated(grpc::ServerContext *ctx) {
    if (public_key.empty())
      return grpc::Status::OK;
    return rpc_authorize(ctx, public_key);
  }
  grpc::Status result(VoidResult r) {
    return r ? grpc::Status::OK
             : grpc::Status(grpc::StatusCode::ABORTED, r.error().message);
  }
  grpc::Status RegisterRuntime(grpc::ServerContext *ctx,
                               const proto::RuntimeRegistration *req,
                               proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    return result(agent.register_runtime(*req));
  }
  grpc::Status PrepareMigration(grpc::ServerContext *ctx,
                                const proto::MigrationPreparation *req,
                                proto::Empty *) override {
    auto auth = public_key.empty()
                    ? grpc::Status::OK
                    : rpc_authorize(ctx, public_key, req->source_node_id());
    if (!auth.ok())
      return auth;
    return result(agent.prepare_migration(*req));
  }
  grpc::Status RestoreCheckpoint(grpc::ServerContext *ctx,
                                 const proto::RecoveryPlan *req,
                                 proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    return result(agent.restore(*req));
  }
  grpc::Status ExecuteMigration(grpc::ServerContext *ctx,
                                const proto::MigrationExecution *req,
                                proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    return result(agent.execute_migration(*req));
  }
  grpc::Status Reclaim(grpc::ServerContext *ctx, const proto::ReclaimEvent *req,
                       proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    ReclaimNotification n;
    n.job_id = req->job_id();
    n.deadline_s = req->tau_seconds();
    n.event_timestamp_ns = req->timestamp();
    n.type = req->type() == "EMERGENCY" || req->type() == "emergency"
                 ? ReclaimType::EMERGENCY
                 : ReclaimType::SCHEDULED;
    return result(agent.handle_reclaim(n));
  }
  grpc::Status ApplyCheckpointInterval(grpc::ServerContext *ctx,
                                       const proto::CheckpointInterval *req,
                                       proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    agent.apply_checkpoint(*req);
    return grpc::Status::OK;
  }
  grpc::Status ApplyKillSwitch(grpc::ServerContext *ctx,
                               const proto::KillSwitchCommand *req,
                               proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    return result(agent.apply_kill(*req));
  }
  grpc::Status ApplyMigrationDecision(grpc::ServerContext *ctx,
                                      const proto::MigrationDecision *req,
                                      proto::Empty *) override {
    auto auth = authenticated(ctx);
    if (!auth.ok())
      return auth;
    ReclaimNotification n;
    n.job_id = req->job_id();
    n.deadline_s = req->deadline_s();
    return result(agent.handle_reclaim(n));
  }
  grpc::Status HealthCheck(grpc::ServerContext *, const proto::Empty *,
                           proto::HealthStatus *resp) override {
    resp->set_healthy(agent.running());
    resp->set_version("0.1.0");
    resp->set_uptime_s((now_ns() - started) / 1000000000);
    return grpc::Status::OK;
  }
};
#else
struct AgentServiceHost::Impl {
  NodeAgent &agent;
  explicit Impl(NodeAgent &a) : agent(a) {}
};
#endif
AgentServiceHost::AgentServiceHost(NodeAgent &a)
    : impl_(std::make_unique<Impl>(a)) {}
AgentServiceHost::~AgentServiceHost() { stop(); }
VoidResult AgentServiceHost::start() {
#if RECLAIMNET_HAVE_PROTO
  const auto &cfg = impl_->agent.config();
  if (!cfg.jwt_public_key_path.empty()) {
    auto key = read_pem_file(cfg.jwt_public_key_path);
    if (!key)
      return key.error();
    impl_->public_key = key.value();
  }
  auto credentials = insecure_server_credentials();
  if (cfg.mtls_enabled) {
    auto tls =
        load_tls_bundle(cfg.tls_cert_path, cfg.tls_key_path, cfg.tls_ca_path);
    if (!tls)
      return tls.error();
    auto result = make_server_credentials(tls.value());
    if (!result)
      return result.error();
    credentials = result.value();
  }
  grpc::ServerBuilder builder;
  builder.AddListeningPort(cfg.listen_address, credentials);
  builder.RegisterService(impl_.get());
  impl_->server = builder.BuildAndStart();
  return impl_->server ? ok_void() : err_void("Node agent RPC listen failed");
#else
  return Error{"gRPC transport is required to run a node agent"};
#endif
}
void AgentServiceHost::stop() {
#if RECLAIMNET_HAVE_PROTO
  if (impl_->server) {
    impl_->server->Shutdown();
    impl_->server.reset();
  }
#endif
}
}
