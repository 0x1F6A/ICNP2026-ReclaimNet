

#include "reclaimnet/grpc_client.h"
#include "reclaimnet/log.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/time.h"

#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC
#include <grpcpp/grpcpp.h>
#endif
#if defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
#include <reclaimnet.grpc.pb.h>
#endif

#include <chrono>
#include <cmath>

namespace reclaimnet {
namespace agent {

CoordinatorClient::CoordinatorClient(std::string coordinator_addr,
                                     bool mtls_enabled,
                                     const std::string &cert_path,
                                     const std::string &key_path,
                                     const std::string &ca_path)
    : addr_(std::move(coordinator_addr)) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC
  std::shared_ptr<grpc::ChannelCredentials> creds;
  if (mtls_enabled) {
    auto bundle = load_tls_bundle(cert_path, key_path, ca_path);
    if (!bundle) {
      initialization_error_ = bundle.error().message;
      return;
    }
    auto result = make_client_credentials(bundle.value());
    if (!result) {
      initialization_error_ = result.error().message;
      return;
    }
    creds = result.value();
  } else {
    creds = insecure_channel_credentials();
  }
  channel_ = grpc::CreateChannel(addr_, creds);
  connected_ = static_cast<bool>(channel_);
  LOG_INFO("CoordinatorClient: channel to %s created", addr_.c_str());
#else
  initialization_error_ = "gRPC transport is not built";
  (void)mtls_enabled;
  (void)cert_path;
  (void)key_path;
  (void)ca_path;
#endif
}

VoidResult CoordinatorClient::ensure_connected() const {
  if (!initialization_error_.empty())
    return Error{initialization_error_};
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (!channel_)
    return Error{"Coordinator channel is not initialized"};
  return ok_void();
#else
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

std::vector<CheckpointPlan> CoordinatorClient::checkpoint_plans() const {
  std::lock_guard<std::mutex> lock(plans_mutex_);
  return checkpoint_plans_;
}

VoidResult CoordinatorClient::register_node(
    const std::string &node_id, const std::string &address,
    const std::string &building, const std::string &switch_id,
    uint64_t total_vram_bytes, uint64_t link_capacity_bps, uint32_t gpu_count,
    double cuda_capability, uint16_t migration_port) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (auto result = ensure_connected(); !result)
    return result.error();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::RegisterNodeRequest req;
  req.set_node_id(node_id);
  req.set_address(address);
  req.set_building(building);
  req.set_switch_id(switch_id);
  req.set_total_vram_bytes(total_vram_bytes);
  req.set_link_capacity_bps(link_capacity_bps);
  req.set_gpu_count(gpu_count);
  req.set_cuda_capability(cuda_capability);
  req.set_migration_port(migration_port);
  proto::RegisterNodeResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = stub->RegisterNode(&ctx, req, &resp);
  if (!status.ok())
    return Error{"RegisterNode RPC failed: " + status.error_message()};
  if (!resp.success())
    return Error{"RegisterNode rejected: " + resp.error_message()};
  {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    access_token_ = resp.access_token();
  }
  return ok_void();
#else
  LOG_WARN("register_node: gRPC/proto not available");
  (void)node_id;
  (void)address;
  (void)building;
  (void)switch_id;
  (void)total_vram_bytes;
  (void)link_capacity_bps;
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

VoidResult CoordinatorClient::register_job(
    const std::string &job_id, const std::string &node_id, uint64_t vram_bytes,
    uint64_t checkpoint_size_bytes, const std::string &user,
    const std::string &tag, double write_cap_bps,
    std::optional<double> loss_budget_s, double lambda_e) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (auto result = ensure_connected(); !result)
    return result.error();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::RegisterJobRequest req;
  req.set_job_id(job_id);
  req.set_node_id(node_id);
  req.set_vram_bytes(vram_bytes);
  req.set_checkpoint_size_bytes(checkpoint_size_bytes);
  req.set_user(user);
  req.set_workload_tag(tag);
  req.set_write_cap_bps(write_cap_bps);
  if (loss_budget_s)
    req.set_loss_budget_s(*loss_budget_s);
  req.set_lambda_e(lambda_e);
  proto::RegisterJobResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = stub->RegisterJob(&ctx, req, &resp);
  if (!status.ok())
    return Error{"RegisterJob RPC failed: " + status.error_message()};
  if (!resp.success())
    return Error{"RegisterJob rejected: " + resp.error_message()};
  return ok_void();
#else
  LOG_WARN("register_job: gRPC/proto not available");
  (void)job_id;
  (void)node_id;
  (void)vram_bytes;
  (void)checkpoint_size_bytes;
  (void)user;
  (void)tag;
  (void)write_cap_bps;
  (void)loss_budget_s;
  (void)lambda_e;
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

VoidResult CoordinatorClient::heartbeat(const std::string &node_id,
                                        const LocalTelemetry &tel) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (auto result = ensure_connected(); !result)
    return result.error();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::HeartbeatRequest req;
  req.set_node_id(node_id);
  req.set_timestamp(tel.sampled_ns);
  auto *t = req.mutable_telemetry();
  t->set_gpu_util_pct(tel.gpu_util_pct);
  t->set_vram_used_pct(tel.vram_used_pct);
  t->set_vram_free_bytes(tel.vram_free_bytes);
  t->set_link_tx_bps(tel.link_tx_bps);
  t->set_link_rx_bps(tel.link_rx_bps);
  t->set_cpu_load_1m(tel.cpu_load_1m);
  t->set_sampled_ns(tel.sampled_ns);
  t->set_gpu_free_count(tel.gpu_free_count);
  t->set_cuda_capability(tel.cuda_capability);
  proto::HeartbeatResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = stub->Heartbeat(&ctx, req, &resp);
  if (!status.ok())
    return Error{"Heartbeat RPC failed: " + status.error_message()};
  if (!resp.ok())
    return Error{"Heartbeat rejected by coordinator"};
  std::vector<CheckpointPlan> plans;
  plans.reserve(resp.checkpoint_intervals_size());
  for (const auto &interval : resp.checkpoint_intervals()) {
    plans.push_back({interval.job_id(), interval.interval_ns(),
                     interval.allocated_bps(), interval.write_rate_limit_bps(),
                     interval.feasible()});
  }
  {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    checkpoint_plans_ = std::move(plans);
    control_response_ = resp;
    if (!resp.refreshed_token().empty())
      access_token_ = resp.refreshed_token();
  }
  return ok_void();
#else
  (void)node_id;
  (void)tel;
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

Result<ReclaimResponse>
CoordinatorClient::reclaim_signal(const ReclaimNotification &notif,
                                  const std::string &node_id) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (auto result = ensure_connected(); !result)
    return result.error();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::ReclaimSignalRequest req;
  req.set_node_id(node_id);
  req.set_job_id(notif.job_id);
  req.set_deadline_s(notif.deadline_s);
  req.set_event_timestamp_ns(notif.event_timestamp_ns);
  switch (notif.type) {
  case ReclaimType::SCHEDULED:
    req.set_reclaim_type(proto::RECLAIM_SCHEDULED);
    break;
  case ReclaimType::EMERGENCY:
    req.set_reclaim_type(proto::RECLAIM_EMERGENCY);
    break;
  default:
    return Error{"Unknown reclaim type"};
  }
  proto::ReclaimSignalResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = stub->ReclaimSignal(&ctx, req, &resp);
  if (!status.ok())
    return Error{"ReclaimSignal RPC failed: " + status.error_message()};
  if (!resp.success())
    return Error{"ReclaimSignal rejected: " + resp.error_message()};
  if (!resp.recovery() &&
      (resp.dst_node_id().empty() || !std::isfinite(resp.allocated_bps()) ||
       resp.allocated_bps() <= 0.0 || !std::isfinite(resp.deadline_s()) ||
       resp.deadline_s() <= 0.0))
    return Error{"Coordinator returned an invalid migration admission"};
  ReclaimResponse r;
  r.dst_node = resp.dst_node_id();
  r.dst_host = resp.destination_address();
  auto colon = r.dst_host.rfind(':');
  if (colon != std::string::npos)
    r.dst_host.resize(colon);
  r.dst_port = static_cast<uint16_t>(resp.destination_port());
  r.allocated_bps = resp.allocated_bps();
  r.deadline_s = resp.deadline_s();
  r.deadline_ns = resp.deadline_ns();
  return r;
#else
  (void)notif;
  (void)node_id;
  return Error{"gRPC/proto not available"};
#endif
}

VoidResult CoordinatorClient::migration_progress(const JobId &job_id,
                                                 uint64_t bytes_transferred,
                                                 bool completed) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  if (auto result = ensure_connected(); !result)
    return result.error();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::MigrationProgressRequest req;
  req.set_job_id(job_id);
  req.set_bytes_transferred(bytes_transferred);
  req.set_completed(completed);
  proto::MigrationProgressResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = stub->MigrationProgress(&ctx, req, &resp);
  if (!status.ok())
    return Error{"MigrationProgress RPC failed: " + status.error_message()};
  if (!resp.acknowledged())
    return Error{"Migration progress was not acknowledged"};
  return ok_void();
#else
  (void)job_id;
  (void)bytes_transferred;
  (void)completed;
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

#if RECLAIMNET_HAVE_PROTO
void CoordinatorClient::authorize(grpc::ClientContext &ctx) const {
  std::lock_guard<std::mutex> lock(plans_mutex_);
  if (!access_token_.empty())
    ctx.AddMetadata("authorization", "Bearer " + access_token_);
}
proto::HeartbeatResponse CoordinatorClient::control_response() const {
  std::lock_guard<std::mutex> lock(plans_mutex_);
  return control_response_;
}
VoidResult
CoordinatorClient::register_runtime(const proto::RuntimeRegistration &r,
                                    const NodeId &node) {
  proto::RegisterJobRequest req;
  req.set_job_id(r.job().job_id());
  req.set_node_id(node);
  req.set_vram_bytes(r.job().vram_bytes());
  req.set_checkpoint_size_bytes(r.job().checkpoint_size());
  req.set_lambda_e(r.job().lambda_e());
  req.set_write_cap_bps(r.job().write_cap_bps());
  if (r.loss_budget_s() > 0)
    req.set_loss_budget_s(r.loss_budget_s());
  req.set_cuda_capability_required(r.cuda_capability_required());
  req.set_gpu_count_required(r.gpu_count_required());
  req.set_remaining_runtime_s(r.remaining_runtime_s());
  *req.mutable_runtime() = r.runtime();
  for (const auto &n : r.rank_nodes())
    req.add_rank_nodes(n);
  req.set_rank_count(r.rank_count());
  proto::RegisterJobResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->RegisterJob(
      &ctx, req, &resp);
  if (!status.ok())
    return Error{status.error_message()};
  return resp.success() ? ok_void() : err_void(resp.error_message());
}
Result<proto::BatchMigrationResponse>
CoordinatorClient::migration_batch(const proto::BatchMigrationRequest &req) {
  proto::BatchMigrationResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->MigrationBatch(
      &ctx, req, &resp);
  if (!status.ok())
    return Error{status.error_message()};
  return resp;
}
Result<proto::ReclaimSignalResponse>
CoordinatorClient::migration_decision(const ReclaimNotification &n,
                                      const NodeId &node) {
  proto::ReclaimSignalRequest req;
  req.set_node_id(node);
  req.set_job_id(n.job_id);
  req.set_deadline_s(n.deadline_s);
  req.set_event_timestamp_ns(n.event_timestamp_ns);
  req.set_reclaim_type(n.type == ReclaimType::EMERGENCY
                           ? proto::RECLAIM_EMERGENCY
                           : proto::RECLAIM_SCHEDULED);
  proto::ReclaimSignalResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->ReclaimSignal(
      &ctx, req, &resp);
  if (!status.ok())
    return Error{status.error_message()};
  return resp;
}
Result<proto::CheckpointInterval>
CoordinatorClient::begin_checkpoint(const proto::CheckpointRequest &req) {
  proto::CheckpointInterval resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->BeginCheckpoint(
      &ctx, req, &resp);
  if (!status.ok())
    return Error{status.error_message()};
  return resp;
}
VoidResult
CoordinatorClient::commit_checkpoint(const proto::CheckpointInfo &req) {
  proto::Empty resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->CommitCheckpoint(
      &ctx, req, &resp);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
Result<proto::RateUpdate> CoordinatorClient::prepare_final_checkpoint(
    const proto::FinalCheckpointRequest &req) {
  proto::RateUpdate resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status =
      proto::CoordinatorService::NewStub(channel_)->PrepareFinalCheckpoint(
          &ctx, req, &resp);
  if (!status.ok())
    return Error{status.error_message()};
  return resp;
}
VoidResult
CoordinatorClient::source_stopped(const proto::SourceStopResult &req) {
  proto::Empty resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->SourceStopped(
      &ctx, req, &resp);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
VoidResult
CoordinatorClient::report_telemetry(const proto::TelemetryReport &req) {
  proto::Empty resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->ReportTelemetry(
      &ctx, req, &resp);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
VoidResult
CoordinatorClient::recovery_complete(const proto::RecoveryResult &req) {
  proto::Empty resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->RecoveryComplete(
      &ctx, req, &resp);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
VoidResult CoordinatorClient::migration_progress(
    const proto::MigrationProgressRequest &req) {
  proto::MigrationProgressResponse resp;
  grpc::ClientContext ctx;
  authorize(ctx);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto status = proto::CoordinatorService::NewStub(channel_)->MigrationProgress(
      &ctx, req, &resp);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
#endif

}
}
