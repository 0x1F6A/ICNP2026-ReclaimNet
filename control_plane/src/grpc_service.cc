


#include "reclaimnet/grpc_service.h"
#include "reclaimnet/log.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/time.h"

#if HAVE_GRPC && HAVE_PROTO
#  include <grpcpp/server_builder.h>
#  include <grpcpp/server.h>
#endif

namespace reclaimnet {
namespace control {

#if HAVE_PROTO



CoordinatorServiceImpl::CoordinatorServiceImpl(Coordinator& coord)
  : coord_(coord) {}

grpc::Status CoordinatorServiceImpl::RegisterNode(
    grpc::ServerContext*, const proto::RegisterNodeRequest* req,
    proto::RegisterNodeResponse* resp) {
  NodeRecord rec{};
  rec.node_id          = req->node_id();
  rec.address          = req->address();
  rec.building         = req->building();
  rec.switch_id        = req->switch_id();
  rec.total_vram_bytes = (uint64_t)req->total_vram_bytes();
  rec.link_capacity_bps= (BandwidthBps)req->link_capacity_bps();
  rec.state            = NodeState::ACTIVE;
  rec.registered_at_ns = now_ns();
  rec.last_heartbeat_ns= now_ns();

  auto r = coord_.register_node(rec);
  if (!r) {
    resp->set_success(false);
    resp->set_error_message(r.error().msg);
    return grpc::Status(grpc::StatusCode::INTERNAL, r.error().msg);
  }
  resp->set_success(true);
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::RegisterJob(
    grpc::ServerContext*, const proto::RegisterJobRequest* req,
    proto::RegisterJobResponse* resp) {
  JobRecord rec{};
  rec.job_id          = req->job_id();
  rec.node_id         = req->node_id();
  rec.vram_bytes      = (uint64_t)req->vram_bytes();
  rec.user            = req->user();
  rec.workload_tag    = req->workload_tag();
  rec.state           = JobState::RUNNING;
  rec.submit_time_ns  = now_ns();
  rec.last_update_ns  = now_ns();
  rec.checkpoint_size = (uint64_t)req->checkpoint_size_bytes();

  auto r = coord_.register_job(rec);
  if (!r) {
    resp->set_success(false);
    resp->set_error_message(r.error().msg);
    return grpc::Status(grpc::StatusCode::INTERNAL, r.error().msg);
  }
  resp->set_success(true);
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::Heartbeat(
    grpc::ServerContext*, const proto::HeartbeatRequest* req,
    proto::HeartbeatResponse* resp) {
  NodeTelemetry tel{};
  if (req->has_telemetry()) {
    const auto& t    = req->telemetry();
    tel.gpu_util_pct  = t.gpu_util_pct();
    tel.vram_used_pct = t.vram_used_pct();
    tel.vram_free_bytes = (uint64_t)t.vram_free_bytes();
    tel.link_tx_bps   = t.link_tx_bps();
    tel.link_rx_bps   = t.link_rx_bps();
    tel.cpu_load_1m   = t.cpu_load_1m();
    tel.updated_ns    = now_ns();
  }
  coord_.on_heartbeat(req->node_id(), tel);
  resp->set_server_time_ns(now_ns());
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::ReclaimSignal(
    grpc::ServerContext*, const proto::ReclaimSignalRequest* req,
    proto::ReclaimSignalResponse* resp) {
  ReclaimType rtype = ReclaimType::VOLUNTARY;
  switch (req->reclaim_type()) {
    case proto::RECLAIM_VOLUNTARY:  rtype = ReclaimType::VOLUNTARY; break;
    case proto::RECLAIM_FORCED:     rtype = ReclaimType::FORCED;    break;
    case proto::RECLAIM_EMERGENCY:  rtype = ReclaimType::EMERGENCY; break;
    default: break;
  }
  auto res = coord_.on_reclaim_signal(
      req->node_id(), req->job_id(),
      req->deadline_s(), rtype);
  if (!res) {
    resp->set_success(false);
    resp->set_error_message(res.error().msg);
    return grpc::Status(grpc::StatusCode::ABORTED, res.error().msg);
  }
  const MigrationCommand& cmd = res.value();
  resp->set_success(true);
  resp->set_dst_node_id(cmd.dst_node);
  resp->set_allocated_bps(cmd.allocated_bps);
  resp->set_deadline_s(cmd.deadline_s);
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::MigrationProgress(
    grpc::ServerContext*, const proto::MigrationProgressRequest* req,
    proto::MigrationProgressResponse* resp) {
  coord_.on_migration_progress(req->job_id(),
                                (uint64_t)req->bytes_transferred(),
                                req->completed());
  resp->set_acknowledged(true);
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::GetTopology(
    grpc::ServerContext*, const proto::GetTopologyRequest*,
    proto::GetTopologyResponse* resp) {
  resp->set_node_count((int32_t)coord_.nodes().size());
  return grpc::Status::OK;
}

#endif



VoidResult run_grpc_server(Coordinator& coord,
                            const std::string& listen_addr,
                            bool mtls_enabled,
                            const std::string& cert,
                            const std::string& key,
                            const std::string& ca) {
#if HAVE_GRPC && HAVE_PROTO
  CoordinatorServiceImpl svc(coord);
  grpc::ServerBuilder builder;

  std::shared_ptr<grpc::ServerCredentials> creds;
  if (mtls_enabled && !cert.empty()) {
    auto bundle_res = load_tls_bundle(cert, key, ca);
    if (!bundle_res) return bundle_res.error();
    auto creds_res  = make_server_credentials(bundle_res.value());
    if (!creds_res) return creds_res.error();
    creds = creds_res.value();
  } else {
    creds = insecure_server_credentials();
  }
  builder.AddListeningPort(listen_addr, creds);
  builder.RegisterService(&svc);

  auto server = builder.BuildAndStart();
  if (!server) return Error{"Failed to start gRPC server on " + listen_addr};

  LOG_INFO("gRPC server listening on %s (mtls=%d)", listen_addr.c_str(), (int)mtls_enabled);
  server->Wait();
  return ok_void();
#else
  (void)coord; (void)listen_addr; (void)mtls_enabled;
  (void)cert; (void)key; (void)ca;
  LOG_WARN("gRPC not available; run_grpc_server is a no-op");
  return ok_void();
#endif
}

}
}
