


#include "reclaimnet/grpc_client.h"
#include "reclaimnet/log.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/time.h"

#if __has_include(<grpcpp/grpcpp.h>)
#  include <grpcpp/grpcpp.h>
#  define HAVE_GRPC 1
#else
#  define HAVE_GRPC 0
#endif

#if HAVE_GRPC && __has_include(<reclaimnet.grpc.pb.h>)
#  include <reclaimnet.grpc.pb.h>
#  define HAVE_PROTO 1
#else
#  define HAVE_PROTO 0
#endif

namespace reclaimnet {
namespace agent {

CoordinatorClient::CoordinatorClient(
    std::string coordinator_addr,
    bool mtls_enabled,
    const std::string& cert_path,
    const std::string& key_path,
    const std::string& ca_path)
  : addr_(std::move(coordinator_addr)) {
#if HAVE_GRPC
  std::shared_ptr<grpc::ChannelCredentials> creds;
  if (mtls_enabled && !cert_path.empty()) {
    auto bundle = load_tls_bundle(cert_path, key_path, ca_path);
    if (bundle) {
      auto cr = make_client_credentials(bundle.value());
      if (cr) creds = cr.value();
    }
  }
  if (!creds) creds = insecure_channel_credentials();
  channel_   = grpc::CreateChannel(addr_, creds);
  connected_ = true;
  LOG_INFO("CoordinatorClient: channel to %s created", addr_.c_str());
#else
  LOG_WARN("CoordinatorClient: gRPC not available, client is stub-only");
  (void)mtls_enabled; (void)cert_path; (void)key_path; (void)ca_path;
#endif
}

void CoordinatorClient::ensure_connected() {
#if HAVE_GRPC
  if (!channel_) {
    channel_   = grpc::CreateChannel(addr_, insecure_channel_credentials());
    connected_ = true;
  }
#endif
}



VoidResult CoordinatorClient::register_node(
    const std::string& node_id, const std::string& address,
    const std::string& building, const std::string& switch_id,
    uint64_t total_vram_bytes, uint64_t link_capacity_bps) {
#if HAVE_GRPC && HAVE_PROTO
  ensure_connected();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::RegisterNodeRequest req;
  req.set_node_id(node_id);
  req.set_address(address);
  req.set_building(building);
  req.set_switch_id(switch_id);
  req.set_total_vram_bytes((int64_t)total_vram_bytes);
  req.set_link_capacity_bps((int64_t)link_capacity_bps);
  proto::RegisterNodeResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->RegisterNode(&ctx, req, &resp);
  if (!status.ok()) return Error{"RegisterNode RPC failed: " + status.error_message()};
  if (!resp.success()) return Error{"RegisterNode rejected: " + resp.error_message()};
  return ok_void();
#else
  LOG_WARN("register_node: gRPC/proto not available");
  (void)node_id; (void)address; (void)building; (void)switch_id;
  (void)total_vram_bytes; (void)link_capacity_bps;
  return ok_void();
#endif
}



VoidResult CoordinatorClient::register_job(
    const std::string& job_id, const std::string& node_id,
    uint64_t vram_bytes, uint64_t checkpoint_size_bytes,
    const std::string& user, const std::string& tag) {
#if HAVE_GRPC && HAVE_PROTO
  ensure_connected();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::RegisterJobRequest req;
  req.set_job_id(job_id);
  req.set_node_id(node_id);
  req.set_vram_bytes((int64_t)vram_bytes);
  req.set_checkpoint_size_bytes((int64_t)checkpoint_size_bytes);
  req.set_user(user);
  req.set_workload_tag(tag);
  proto::RegisterJobResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->RegisterJob(&ctx, req, &resp);
  if (!status.ok()) return Error{"RegisterJob RPC failed: " + status.error_message()};
  if (!resp.success()) return Error{"RegisterJob rejected: " + resp.error_message()};
  return ok_void();
#else
  LOG_WARN("register_job: gRPC/proto not available");
  (void)job_id; (void)node_id; (void)vram_bytes; (void)checkpoint_size_bytes;
  (void)user; (void)tag;
  return ok_void();
#endif
}



VoidResult CoordinatorClient::heartbeat(const std::string& node_id,
                                         const LocalTelemetry& tel) {
#if HAVE_GRPC && HAVE_PROTO
  ensure_connected();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::HeartbeatRequest req;
  req.set_node_id(node_id);
  auto* t = req.mutable_telemetry();
  t->set_gpu_util_pct(tel.gpu_util_pct);
  t->set_vram_used_pct(tel.vram_used_pct);
  t->set_vram_free_bytes((int64_t)tel.vram_free_bytes);
  t->set_link_tx_bps(tel.link_tx_bps);
  t->set_link_rx_bps(tel.link_rx_bps);
  t->set_cpu_load_1m(tel.cpu_load_1m);
  proto::HeartbeatResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->Heartbeat(&ctx, req, &resp);
  if (!status.ok()) return Error{"Heartbeat RPC failed: " + status.error_message()};
  return ok_void();
#else
  (void)node_id; (void)tel;
  return ok_void();
#endif
}



Result<ReclaimResponse> CoordinatorClient::reclaim_signal(
    const ReclaimNotification& notif, const std::string& node_id) {
#if HAVE_GRPC && HAVE_PROTO
  ensure_connected();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::ReclaimSignalRequest req;
  req.set_node_id(node_id);
  req.set_job_id(notif.job_id);
  req.set_deadline_s(notif.deadline_s);
  switch (notif.type) {
    case ReclaimType::VOLUNTARY:  req.set_reclaim_type(proto::RECLAIM_VOLUNTARY);  break;
    case ReclaimType::FORCED:     req.set_reclaim_type(proto::RECLAIM_FORCED);     break;
    case ReclaimType::EMERGENCY:  req.set_reclaim_type(proto::RECLAIM_EMERGENCY);  break;
    default: break;
  }
  proto::ReclaimSignalResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->ReclaimSignal(&ctx, req, &resp);
  if (!status.ok()) return Error{"ReclaimSignal RPC failed: " + status.error_message()};
  if (!resp.success()) return Error{"ReclaimSignal rejected: " + resp.error_message()};
  ReclaimResponse r;
  r.dst_node     = resp.dst_node_id();
  r.dst_port     = 7600;
  r.allocated_bps= resp.allocated_bps();
  r.deadline_s   = resp.deadline_s();
  return r;
#else
  (void)notif; (void)node_id;
  return Error{"gRPC/proto not available"};
#endif
}



VoidResult CoordinatorClient::migration_progress(
    const JobId& job_id, uint64_t bytes_transferred, bool completed) {
#if HAVE_GRPC && HAVE_PROTO
  ensure_connected();
  auto stub = proto::CoordinatorService::NewStub(channel_);
  proto::MigrationProgressRequest req;
  req.set_job_id(job_id);
  req.set_bytes_transferred((int64_t)bytes_transferred);
  req.set_completed(completed);
  proto::MigrationProgressResponse resp;
  grpc::ClientContext ctx;
  auto status = stub->MigrationProgress(&ctx, req, &resp);
  if (!status.ok()) return Error{"MigrationProgress RPC failed: " + status.error_message()};
  return ok_void();
#else
  (void)job_id; (void)bytes_transferred; (void)completed;
  return ok_void();
#endif
}

}
}
