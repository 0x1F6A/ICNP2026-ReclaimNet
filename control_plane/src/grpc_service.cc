#include "reclaimnet/grpc_service.h"
#include "reclaimnet/log.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/prometheus.h"
#include "reclaimnet/rpc_auth.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#if RECLAIMNET_HAVE_PROTO
#include <google/protobuf/util/json_util.h>
#endif

#include <cmath>
#include <limits>

#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#endif

namespace reclaimnet {
namespace control {

#if defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
namespace {
grpc::Status invalid(const std::string &message) {
  return {grpc::StatusCode::INVALID_ARGUMENT, message};
}

bool nonnegative_finite(double value) {
  return std::isfinite(value) && value >= 0.0;
}

uint64_t bandwidth_value(double value) {
  if (!nonnegative_finite(value))
    return 0;
  if (value >= static_cast<double>(std::numeric_limits<uint64_t>::max()))
    return std::numeric_limits<uint64_t>::max();
  return static_cast<uint64_t>(value);
}
}

namespace {
void fill_checkpoint(const CheckpointCommand &c, proto::CheckpointInterval *p) {
  p->set_job_id(c.job_id);
  p->set_interval_ns(bandwidth_value(c.interval_s * 1e9));
  p->set_allocated_bps(bandwidth_value(c.allocated_bps));
  p->set_write_rate_limit_bps(bandwidth_value(c.write_rate_limit_bps));
  p->set_exact_allocated_bps(c.allocated_bps);
  p->set_exact_write_rate_limit_bps(c.write_rate_limit_bps);
  p->set_feasible(c.feasible);
  p->set_replica_node_id(c.replica_node_id);
  p->set_replica_address(c.replica_address);
  p->set_replica_port(c.replica_port);
  p->set_eligible_start_ns(c.eligible_start_ns);
  p->set_checkpoint_id(c.checkpoint_id);
  p->set_rank(c.rank);
  p->set_gang_size(c.gang_size);
  p->set_checkpoint_sequence(c.checkpoint_sequence);
  p->set_deadline_ns(c.deadline_ns);
  p->set_allocation_version(c.allocation_version);
}
void fill_recovery(const Coordinator &coord, const ReplicaCheckpoint &c,
                   proto::RecoveryPlan *p) {
  p->set_job_id(c.job_id);
  p->set_checkpoint_id(c.checkpoint_id);
  p->set_replica_node_id(c.replica_node_id);
  auto node = coord.nodes().get(c.replica_node_id);
  if (node)
    p->set_replica_address(node->address);
  p->set_path(c.path);
  p->set_checksum_sha256(c.checksum_sha256);
  p->set_size_bytes(c.size_bytes);
  p->set_sequence(c.sequence);
  p->set_rank(c.rank);
  p->set_gang_size(c.gang_size);
  if (auto job = coord.jobs().get(c.job_id)) {
    const auto ranks = std::max(1u, job->rank_count);
    p->set_vram_bytes(job->vram_bytes / ranks +
                      (c.rank < job->vram_bytes % ranks));
    p->set_gpu_count_required(job->gpu_count_required);
    p->set_cuda_capability_required(job->cuda_capability_required);
  }
  auto runtime = coord.runtime_spec(c.job_id);
  if (!runtime.empty()) {
    auto parsed = google::protobuf::util::JsonStringToMessage(
        runtime, p->mutable_runtime());
    if (!parsed.ok())
      p->clear_runtime();
  }
}
void fill_migration(const Coordinator &coord, const MigrationCommand &c,
                    proto::ReclaimSignalResponse *p) {
  p->set_success(true);
  p->set_dst_node_id(c.dst_node);
  p->set_allocated_bps(c.allocated_bps);
  p->set_deadline_s(c.deadline_s);
  p->set_deadline_ns(c.deadline_ns);
  p->set_destination_address(c.destination_address);
  p->set_destination_port(c.destination_port);
  p->set_migration_id(c.migration_id);
  p->set_eligible_start_ns(c.eligible_start_ns);
  p->set_recovery(c.recovery);
  for (const auto &r : c.recoveries)
    fill_recovery(coord, r, p->add_recoveries());
  for (const auto &rank : c.ranks) {
    auto *r = p->add_ranks();
    r->set_rank(rank.rank);
    r->set_source_node_id(rank.source_node);
    auto source = coord.nodes().get(rank.source_node);
    if (source)
      r->set_source_address(source->address);
    r->set_destination_node_id(rank.destination_node);
    r->set_destination_address(rank.destination_address);
    r->set_destination_port(rank.destination_port);
    r->set_allocated_bps(rank.allocated_bps);
    r->set_eligible_start_ns(rank.eligible_start_ns);
    r->set_deadline_ns(c.deadline_ns);
    r->set_restore_time_s(rank.restore_time_s);
    r->set_checkpoint_sequence(rank.checkpoint_sequence);
    r->set_allocation_version(rank.allocation_version);
  }
}
}

CoordinatorServiceImpl::CoordinatorServiceImpl(Coordinator &coord)
    : coord_(coord) {
  if (coord.config().jwt_enabled) {
    auto public_key = read_pem_file(coord.config().jwt_public_key_path);
    auto private_key = read_pem_file(coord.config().jwt_private_key_path);
    if (public_key)
      jwt_public_key_ = public_key.value();
    if (private_key)
      jwt_private_key_ = private_key.value();
  }
}
grpc::Status CoordinatorServiceImpl::authenticate(grpc::ServerContext *ctx,
                                                  const NodeId &node) {
  if (!coord_.config().jwt_enabled)
    return grpc::Status::OK;
  return rpc_authorize(ctx, jwt_public_key_, node);
}
std::string CoordinatorServiceImpl::token(const NodeId &node) const {
  if (!coord_.config().jwt_enabled)
    return {};
  auto issued = now_ns() / 1000000000;
  std::lock_guard<std::mutex> lock(token_mutex_);
  auto cached = tokens_.find(node);
  if (cached != tokens_.end() &&
      cached->second.first - issued > coord_.config().jwt_lifetime_s / 2)
    return cached->second.second;
  auto result = issue_jwt_rs256(
      {node, node, issued, issued + coord_.config().jwt_lifetime_s},
      jwt_private_key_);
  if (!result)
    return {};
  tokens_[node] = {issued + coord_.config().jwt_lifetime_s, result.value()};
  return result.value();
}

grpc::Status CoordinatorServiceImpl::Register(grpc::ServerContext *ctx,
                                              const proto::RegisterRequest *req,
                                              proto::RegisterResponse *resp) {
  proto::RegisterNodeRequest registration;
  registration.set_node_id(req->node_id());
  registration.set_address(req->address());
  registration.set_building(req->building());
  registration.set_switch_id(req->switch_id());
  registration.set_total_vram_bytes(req->total_vram());
  registration.set_link_capacity_bps(req->link_capacity());
  proto::RegisterNodeResponse result;
  auto status = RegisterNode(ctx, &registration, &result);
  resp->set_accepted(result.success());
  resp->set_message(result.error_message());
  return status;
}

grpc::Status
CoordinatorServiceImpl::RegisterNode(grpc::ServerContext *ctx,
                                     const proto::RegisterNodeRequest *req,
                                     proto::RegisterNodeResponse *resp) {
  if (req->node_id().empty() || req->address().empty() ||
      req->total_vram_bytes() == 0 || req->link_capacity_bps() == 0)
    return invalid("Node id, address, VRAM and link capacity are required");
  if (coord_.config().jwt_enabled && !rpc_peer_identity(ctx, req->node_id()))
    return {grpc::StatusCode::PERMISSION_DENIED,
            "Registration identity does not match the client certificate"};
  NodeRecord rec{};
  rec.node_id = req->node_id();
  rec.address = req->address();
  rec.building = req->building();
  rec.switch_id = req->switch_id();
  rec.total_vram_bytes = req->total_vram_bytes();
  rec.link_capacity_bps = req->link_capacity_bps();
  rec.gpu_count = std::max(1u, req->gpu_count());
  rec.gpu_free_count = rec.gpu_count;
  rec.cuda_capability = req->cuda_capability();
  rec.state = NodeState::ONLINE;
  rec.registered_at_ns = now_ns();
  rec.last_heartbeat_ns = rec.registered_at_ns;

  auto result = coord_.register_node(
      rec, req->migration_port() ? req->migration_port() : 7600);
  if (!result) {
    resp->set_error_message(result.error().message);
    return {grpc::StatusCode::INTERNAL, result.error().message};
  }
  resp->set_success(true);
  resp->set_access_token(token(rec.node_id));
  resp->set_jwt_public_key(jwt_public_key_);
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::RegisterJob(grpc::ServerContext *ctx,
                                    const proto::RegisterJobRequest *req,
                                    proto::RegisterJobResponse *resp) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  if (req->job_id().empty() || req->node_id().empty() ||
      req->vram_bytes() == 0 || req->checkpoint_size_bytes() == 0)
    return invalid(
        "Job id, source node, VRAM and checkpoint size are required");
  if (!nonnegative_finite(req->write_cap_bps()) ||
      !nonnegative_finite(req->lambda_e()) ||
      (req->has_loss_budget_s() &&
       (!std::isfinite(req->loss_budget_s()) || req->loss_budget_s() <= 0.0)))
    return invalid("Invalid job bandwidth cap, hazard rate or loss budget");
  auto node = coord_.nodes().get(req->node_id());
  if (!node)
    return {grpc::StatusCode::NOT_FOUND, "Source node is not registered"};
  auto ranks = std::max(1u, req->rank_count());
  if ((req->vram_bytes() + ranks - 1) / ranks > node->total_vram_bytes)
    return invalid("Job rank VRAM exceeds the source node capacity");

  JobRecord rec{};
  rec.job_id = req->job_id();
  rec.node_id = req->node_id();
  rec.vram_bytes = req->vram_bytes();
  rec.user = req->user();
  rec.workload_tag = req->workload_tag();
  rec.state = JobState::RUNNING;
  rec.submit_time_ns = now_ns();
  rec.last_update_ns = rec.submit_time_ns;
  rec.checkpoint_size = req->checkpoint_size_bytes();
  rec.write_cap_bps = req->write_cap_bps();
  rec.lambda_i = req->lambda_e();
  if (req->has_loss_budget_s())
    rec.loss_budget_s = req->loss_budget_s();
  rec.cuda_capability_required = req->cuda_capability_required();
  rec.gpu_count_required = std::max(1u, req->gpu_count_required());
  rec.remaining_runtime_s =
      req->remaining_runtime_s() > 0 ? req->remaining_runtime_s() : 3600;
  rec.rank_nodes.assign(req->rank_nodes().begin(), req->rank_nodes().end());
  rec.rank_count = std::max(1u, req->rank_count());
  std::string runtime_json;
  if (req->has_runtime()) {
    auto encoded = google::protobuf::util::MessageToJsonString(req->runtime(),
                                                               &runtime_json);
    if (!encoded.ok())
      return invalid(encoded.ToString());
  }
  auto result = coord_.register_job(rec, runtime_json);
  if (!result) {
    resp->set_error_message(result.error().message);
    return {grpc::StatusCode::ALREADY_EXISTS, result.error().message};
  }
  resp->set_success(true);
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::Heartbeat(grpc::ServerContext *ctx,
                                  const proto::HeartbeatRequest *req,
                                  proto::HeartbeatResponse *resp) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  auto node = coord_.nodes().get(req->node_id());
  if (!node)
    return {grpc::StatusCode::NOT_FOUND, "Node is not registered"};
  if (req->timestamp() < 0 ||
      (req->has_telemetry() && req->telemetry().sampled_ns() < 0))
    return invalid("Telemetry timestamps must be nonnegative nanoseconds");
  NodeTelemetry tel = node->telemetry;
  if (req->has_telemetry()) {
    const auto &t = req->telemetry();
    if (!nonnegative_finite(t.gpu_util_pct()) || t.gpu_util_pct() > 100.0 ||
        !nonnegative_finite(t.vram_used_pct()) || t.vram_used_pct() > 100.0 ||
        !nonnegative_finite(t.link_tx_bps()) ||
        !nonnegative_finite(t.link_rx_bps()) ||
        !nonnegative_finite(t.cpu_load_1m()) ||
        t.vram_free_bytes() > node->total_vram_bytes)
      return invalid("Invalid node telemetry");
    tel.gpu_free_count = t.gpu_free_count();
    tel.cuda_capability = t.cuda_capability();
    tel.gpu_util_pct = t.gpu_util_pct();
    tel.vram_used_pct = t.vram_used_pct();
    tel.vram_free_bytes = t.vram_free_bytes();
    tel.link_tx_bps = t.link_tx_bps();
    tel.link_rx_bps = t.link_rx_bps();
    tel.cpu_load_1m = t.cpu_load_1m();
  } else {
    if (req->free_vram() > node->total_vram_bytes)
      return invalid("Free VRAM exceeds node capacity");
    tel.vram_free_bytes = req->free_vram();
  }
  const int64_t server_timestamp_ns = now_ns();
  tel.updated_ns =
      req->has_telemetry() && req->telemetry().sampled_ns() > 0
          ? req->telemetry().sampled_ns()
          : (req->timestamp() > 0 ? req->timestamp() : server_timestamp_ns);
  coord_.on_heartbeat(req->node_id(), tel);
  resp->set_ok(true);
  resp->set_server_ts(server_timestamp_ns);
  resp->set_server_time_ns(server_timestamp_ns);
  for (const auto &command : coord_.checkpoint_commands(req->node_id())) {
    auto *interval = resp->add_checkpoint_intervals();
    interval->set_job_id(command.job_id);
    interval->set_interval_ns(bandwidth_value(command.interval_s * 1e9));
    interval->set_allocated_bps(bandwidth_value(command.allocated_bps));
    interval->set_write_rate_limit_bps(
        bandwidth_value(command.write_rate_limit_bps));
    interval->set_feasible(command.feasible);
    interval->set_replica_node_id(command.replica_node_id);
    interval->set_replica_address(command.replica_address);
    interval->set_replica_port(command.replica_port);
    interval->set_checkpoint_id(command.checkpoint_id);
    interval->set_eligible_start_ns(command.eligible_start_ns);
    interval->set_rank(command.rank);
    interval->set_gang_size(command.gang_size);
    interval->set_exact_allocated_bps(command.allocated_bps);
    interval->set_exact_write_rate_limit_bps(command.write_rate_limit_bps);
    interval->set_deadline_ns(command.deadline_ns);
    interval->set_allocation_version(command.allocation_version);
  }
  for (const auto &rate : coord_.rate_updates(req->node_id())) {
    auto *update = resp->add_rate_updates();
    update->set_job_id(rate.job_id);
    update->set_migration_id(rate.migration_id);
    update->set_rank(rate.rank);
    update->set_allocated_bps(rate.allocated_bps);
    update->set_allocation_version(rate.allocation_version);
    update->set_eligible_start_ns(rate.eligible_start_ns);
    update->set_deadline_ns(rate.deadline_ns);
  }
  for (const auto &recovery : coord_.recovery_commands(req->node_id()))
    fill_recovery(coord_, recovery, resp->add_recoveries());
  for (const auto &stop : coord_.stop_jobs(req->node_id())) {
    auto *command = resp->add_stop_jobs();
    command->set_job_id(stop.first);
    command->set_barrier_id(stop.second);
  }
  for (const auto &start : coord_.start_jobs(req->node_id())) {
    auto *command = resp->add_start_jobs();
    command->set_job_id(start.first);
    command->set_barrier_id(start.second);
  }
  resp->set_refreshed_token(token(req->node_id()));
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::ReportTelemetry(grpc::ServerContext *ctx,
                                        const proto::TelemetryReport *req,
                                        proto::Empty *) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  if (!coord_.nodes().get(req->node_id()))
    return {grpc::StatusCode::NOT_FOUND, "Node is not registered"};
  if (req->timestamp() < 0)
    return invalid("Report timestamp must be nonnegative nanoseconds");
  for (const auto &sample : req->bw_samples()) {
    const auto &node_id =
        sample.node_id().empty() ? req->node_id() : sample.node_id();
    if (node_id != req->node_id() || sample.timestamp() < 0)
      return invalid("Bandwidth sample belongs to another node");
  }
  for (const auto &sample : req->hazard_samples()) {
    const auto &node_id =
        sample.node_id().empty() ? req->node_id() : sample.node_id();
    if (node_id != req->node_id() || !nonnegative_finite(sample.lambda_e()) ||
        sample.timestamp() < 0)
      return invalid("Invalid hazard sample");
  }
  for (const auto &checkpoint : req->checkpoints()) {
    auto job = coord_.jobs().get(checkpoint.job_id());
    if (!job || job->node_id != req->node_id() ||
        (!checkpoint.node_id().empty() &&
         checkpoint.node_id() != req->node_id()))
      return invalid("Checkpoint does not belong to this node");
  }
  for (const auto &sample : req->bw_samples()) {
    if (!sample.dst_node_id().empty()) {
      auto result = coord_.on_bandwidth_sample(
          sample.src_node_id().empty() ? req->node_id() : sample.src_node_id(),
          sample.dst_node_id(), sample.bps(),
          sample.timestamp() > 0 ? sample.timestamp() : now_ns());
      if (!result)
        return invalid(result.error().message);
      continue;
    }
    auto node = coord_.nodes().get(req->node_id());
    auto tel = node->telemetry;
    tel.link_tx_bps = static_cast<double>(sample.bps());
    tel.updated_ns = sample.timestamp() > 0
                         ? sample.timestamp()
                         : (req->timestamp() > 0 ? req->timestamp() : now_ns());
    coord_.on_heartbeat(req->node_id(), tel);
  }
  for (const auto &sample : req->hazard_samples()) {
    const int64_t timestamp =
        sample.timestamp() > 0
            ? sample.timestamp()
            : (req->timestamp() > 0 ? req->timestamp() : now_ns());
    auto result =
        coord_.on_hazard_sample(req->node_id(), sample.lambda_e(), timestamp);
    if (!result)
      return invalid(result.error().message);
  }
  for (const auto &checkpoint : req->checkpoints()) {
    LabelSet labels{{{"node", req->node_id()}, {"job", checkpoint.job_id()}}};
    MetricRegistry::instance()
        .gauge("reclaimnet_checkpoint_bytes",
               "Latest reported checkpoint size in bytes", labels)
        .set(checkpoint.size_bytes());
    MetricRegistry::instance()
        .gauge("reclaimnet_checkpoint_duration_seconds",
               "Latest reported checkpoint duration in seconds", labels)
        .set(static_cast<double>(checkpoint.duration_ns()) / 1e9);
  }
  for (const auto &record : req->flow_records_json()) {
    auto result = coord_.export_flow(record);
    if (!result)
      return invalid(result.error().message);
  }
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::ReclaimSignal(grpc::ServerContext *ctx,
                                      const proto::ReclaimSignalRequest *req,
                                      proto::ReclaimSignalResponse *resp) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  if (!std::isfinite(req->deadline_s()) || req->deadline_s() < 0.0 ||
      (req->deadline_s() == 0.0 &&
       req->reclaim_type() != proto::RECLAIM_EMERGENCY))
    return invalid("Scheduled reclaim requires positive notice; emergency "
                   "notice may be zero");
  if (req->event_timestamp_ns() < 0)
    return invalid("Reclaim timestamp must be nonnegative nanoseconds");
  if (req->reclaim_type() != proto::RECLAIM_SCHEDULED &&
      req->reclaim_type() != proto::RECLAIM_EMERGENCY)
    return invalid("Unknown reclaim type");
  const auto type = req->reclaim_type() == proto::RECLAIM_EMERGENCY
                        ? ReclaimType::EMERGENCY
                        : ReclaimType::SCHEDULED;
  auto result =
      coord_.on_reclaim_signal(req->node_id(), req->job_id(), req->deadline_s(),
                               type, req->event_timestamp_ns());
  if (!result) {
    resp->set_error_message(result.error().message);
    return {grpc::StatusCode::ABORTED, result.error().message};
  }
  fill_migration(coord_, result.value(), resp);
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::ReportReclaim(
    grpc::ServerContext *ctx, const proto::ReclaimEvent *req, proto::Empty *) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  ReclaimType type;
  if (req->type() == "emergency" || req->type() == "EMERGENCY")
    type = ReclaimType::EMERGENCY;
  else if (req->type().empty() || req->type() == "scheduled" ||
           req->type() == "SCHEDULED" || req->type() == "voluntary" ||
           req->type() == "forced")
    type = ReclaimType::SCHEDULED;
  else
    return invalid("Unknown reclaim event type");
  if (req->timestamp() < 0)
    return invalid("Reclaim timestamp must be nonnegative nanoseconds");
  auto result =
      coord_.on_reclaim_event(req->node_id(), req->job_id(), req->tau_seconds(),
                              type, req->timestamp());
  if (!result)
    return invalid(result.error().message);
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::RequestMigration(grpc::ServerContext *ctx,
                                         const proto::MigrationRequest *req,
                                         proto::MigrationDecision *resp) {
  auto job = coord_.jobs().get(req->job_id());
  if (!job || job->node_id != req->src_node_id())
    return {grpc::StatusCode::NOT_FOUND,
            "Job is not registered on the source node"};
  if (req->checkpoint_size() != 0 &&
      req->checkpoint_size() != job->checkpoint_size)
    return invalid("Checkpoint size differs from the registered job");
  if (!nonnegative_finite(req->lambda_e_src()))
    return invalid("Invalid source hazard rate");
  if (req->event_timestamp_ns() < 0)
    return invalid("Migration timestamp must be nonnegative nanoseconds");
  if (req->lambda_e_src() > 0.0) {
    const int64_t timestamp =
        req->event_timestamp_ns() > 0 ? req->event_timestamp_ns() : now_ns();
    auto result = coord_.on_hazard_sample(req->src_node_id(),
                                          req->lambda_e_src(), timestamp);
    if (!result)
      return invalid(result.error().message);
  }
  proto::ReclaimSignalRequest signal;
  signal.set_node_id(req->src_node_id());
  signal.set_job_id(req->job_id());
  signal.set_deadline_s(req->tau_seconds());
  signal.set_event_timestamp_ns(req->event_timestamp_ns());
  signal.set_reclaim_type(proto::RECLAIM_SCHEDULED);
  proto::ReclaimSignalResponse result;
  auto status = ReclaimSignal(ctx, &signal, &result);
  resp->set_job_id(req->job_id());
  resp->set_feasible(status.ok() && result.success());
  if (!status.ok()) {
    resp->set_reject_reason(status.error_message());
    return status;
  }
  resp->set_dst_node_id(result.dst_node_id());
  resp->set_allocated_bps(bandwidth_value(result.allocated_bps()));
  resp->set_exact_allocated_bps(result.allocated_bps());
  resp->set_deadline_s(result.deadline_s());
  resp->set_deadline_ns(result.deadline_ns());
  resp->set_transfer_time(static_cast<double>(job->checkpoint_size) * 8.0 /
                          result.allocated_bps());
  resp->set_survival_prob(coord_.hazard().survival_prob(result.dst_node_id()));
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::UpdateRateBudget(grpc::ServerContext *ctx,
                                         const proto::RateLimit *,
                                         proto::AdmissionResult *resp) {
  if (auto auth = authenticate(ctx); !auth.ok())
    return auth;
  resp->set_admitted(false);
  resp->set_reason("Rates are assigned by migration admission; external rate "
                   "overrides are not supported");
  return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::MigrationProgress(
    grpc::ServerContext *ctx, const proto::MigrationProgressRequest *req,
    proto::MigrationProgressResponse *resp) {
  auto job = coord_.jobs().get(req->job_id());
  if (!job)
    return {grpc::StatusCode::NOT_FOUND, "Job is not registered"};
  if (req->rank() >= job->rank_count ||
      (!job->rank_nodes.empty() && req->rank() >= job->rank_nodes.size()))
    return invalid("Migration rank is not registered");
  const auto source =
      job->rank_nodes.empty() ? job->node_id : job->rank_nodes[req->rank()];
  const bool completed_retry =
      job->state != JobState::MIGRATING && req->completed() && !req->failed() &&
      req->migration_id().rfind("migration:" + req->job_id() + ":", 0) == 0;
  if (auto auth = authenticate(ctx, completed_retry ? NodeId{} : source);
      !auth.ok())
    return auth;
  auto result = coord_.on_migration_progress(
      req->job_id(), req->bytes_transferred(), req->completed(),
      req->migration_id(), req->rank(), req->failed(), req->timestamp_ns());
  if (!result)
    return {grpc::StatusCode::ABORTED, result.error().message};
  resp->set_acknowledged(true);
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::GetTopology(grpc::ServerContext *ctx,
                                    const proto::GetTopologyRequest *,
                                    proto::GetTopologyResponse *resp) {
  if (auto auth = authenticate(ctx); !auth.ok())
    return auth;
  resp->set_node_count(coord_.nodes().size());
  return grpc::Status::OK;
}

grpc::Status
CoordinatorServiceImpl::GetPrometheusSnapshot(grpc::ServerContext *ctx,
                                              const proto::Empty *,
                                              proto::PrometheusSnapshot *resp) {
  if (auto auth = authenticate(ctx); !auth.ok())
    return auth;
  resp->set_metrics_text(MetricRegistry::instance().render());
  return grpc::Status::OK;
}
grpc::Status CoordinatorServiceImpl::PrepareFinalCheckpoint(
    grpc::ServerContext *ctx, const proto::FinalCheckpointRequest *req,
    proto::RateUpdate *resp) {
  auto job = coord_.jobs().get(req->job_id());
  if (!job)
    return {grpc::StatusCode::NOT_FOUND, "Job is not registered"};
  if (req->rank() >= job->rank_count ||
      (!job->rank_nodes.empty() && req->rank() >= job->rank_nodes.size()))
    return invalid("Final checkpoint rank is not registered");
  const auto source =
      job->rank_nodes.empty() ? job->node_id : job->rank_nodes[req->rank()];
  if (req->node_id() != source)
    return {grpc::StatusCode::PERMISSION_DENIED,
            "Final checkpoint source does not own the migration rank"};
  if (auto auth = authenticate(ctx, source); !auth.ok())
    return auth;
  auto result = coord_.prepare_final_payload(req->job_id(), req->migration_id(),
                                             req->rank(), req->size_bytes(),
                                             req->timestamp_ns());
  if (!result)
    return {grpc::StatusCode::ABORTED, result.error().message};
  const auto &entry = result.value();
  if (entry.src != req->node_id())
    return {grpc::StatusCode::PERMISSION_DENIED,
            "Final checkpoint source does not own the migration rank"};
  resp->set_job_id(entry.job_id);
  resp->set_migration_id(entry.migration_id);
  resp->set_rank(entry.rank);
  resp->set_allocated_bps(entry.allocated_bps);
  resp->set_allocation_version(entry.allocation_version);
  resp->set_eligible_start_ns(entry.eligible_start_ns);
  resp->set_deadline_ns(entry.deadline_ns);
  return grpc::Status::OK;
}
grpc::Status
CoordinatorServiceImpl::SourceStopped(grpc::ServerContext *ctx,
                                      const proto::SourceStopResult *req,
                                      proto::Empty *) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  auto result = coord_.source_stopped(req->node_id(), req->job_id(),
                                      req->barrier_id(), req->completed_ns());
  return result
             ? grpc::Status::OK
             : grpc::Status(grpc::StatusCode::ABORTED, result.error().message);
}
grpc::Status
CoordinatorServiceImpl::BeginCheckpoint(grpc::ServerContext *ctx,
                                        const proto::CheckpointRequest *req,
                                        proto::CheckpointInterval *resp) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  auto result = coord_.begin_checkpoint(req->node_id(), req->job_id(),
                                        req->checkpoint_size_bytes(),
                                        req->timestamp_ns(), req->rank());
  if (!result)
    return {grpc::StatusCode::ABORTED, result.error().message};
  fill_checkpoint(result.value(), resp);
  return grpc::Status::OK;
}
grpc::Status
CoordinatorServiceImpl::CommitCheckpoint(grpc::ServerContext *ctx,
                                         const proto::CheckpointInfo *req,
                                         proto::Empty *) {
  if (auto auth = authenticate(ctx, req->replica_node_id()); !auth.ok())
    return auth;
  ReplicaCheckpoint c;
  c.job_id = req->job_id();
  c.checkpoint_id = req->checkpoint_id();
  c.sequence = req->sequence();
  c.size_bytes = req->size_bytes();
  c.completed_at_ns = req->completed_at();
  c.replica_node_id = req->replica_node_id();
  c.path = req->path();
  c.checksum_sha256 = req->checksum_sha256();
  c.rank = req->rank();
  c.gang_size = std::max(1u, req->gang_size());
  c.source_node_ids.assign(req->source_node_ids().begin(),
                           req->source_node_ids().end());
  auto result =
      coord_.commit_checkpoint(c, req->migration_id(), req->restored());
  return result
             ? grpc::Status::OK
             : grpc::Status(grpc::StatusCode::ABORTED, result.error().message);
}
grpc::Status
CoordinatorServiceImpl::RecoveryComplete(grpc::ServerContext *ctx,
                                         const proto::RecoveryResult *req,
                                         proto::Empty *) {
  if (auto auth = authenticate(ctx, req->node_id()); !auth.ok())
    return auth;
  if (!req->restored())
    return invalid("Recovery restore was not acknowledged");
  auto result = coord_.recovery_complete(req->node_id(), req->job_id(),
                                         req->checkpoint_id(), req->rank(),
                                         req->completed_ns());
  return result
             ? grpc::Status::OK
             : grpc::Status(grpc::StatusCode::ABORTED, result.error().message);
}
grpc::Status
CoordinatorServiceImpl::MigrationBatch(grpc::ServerContext *ctx,
                                       const proto::BatchMigrationRequest *req,
                                       proto::BatchMigrationResponse *resp) {
  std::vector<ReclaimRequest> batch;
  for (const auto &request : req->requests()) {
    if (auto auth = authenticate(ctx, request.node_id()); !auth.ok())
      return auth;
    batch.push_back({request.node_id(), request.job_id(), request.deadline_s(),
                     request.reclaim_type() == proto::RECLAIM_EMERGENCY
                         ? ReclaimType::EMERGENCY
                         : ReclaimType::SCHEDULED,
                     request.event_timestamp_ns()});
  }
  auto results = coord_.on_reclaim_batch(batch);
  for (const auto &result : results) {
    auto *decision = resp->add_decisions();
    if (result)
      fill_migration(coord_, result.value(), decision);
    else
      decision->set_error_message(result.error().message);
  }
  return grpc::Status::OK;
}

#endif

VoidResult run_grpc_server(Coordinator &coord, const std::string &listen_addr,
                           bool mtls_enabled, const std::string &cert,
                           const std::string &key, const std::string &ca) {
#if defined(RECLAIMNET_HAVE_GRPC) && RECLAIMNET_HAVE_GRPC &&                   \
    defined(RECLAIMNET_HAVE_PROTO) && RECLAIMNET_HAVE_PROTO
  CoordinatorServiceImpl service(coord);
  grpc::ServerBuilder builder;
  std::shared_ptr<grpc::ServerCredentials> credentials;
  if (mtls_enabled) {
    auto bundle = load_tls_bundle(cert, key, ca);
    if (!bundle)
      return bundle.error();
    auto result = make_server_credentials(bundle.value());
    if (!result)
      return result.error();
    credentials = result.value();
  } else {
    credentials = insecure_server_credentials();
  }
  builder.AddListeningPort(listen_addr, credentials);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server)
    return Error{"Failed to start gRPC server on " + listen_addr};
  LOG_INFO("gRPC server listening on %s (mtls=%d)", listen_addr.c_str(),
           (int)mtls_enabled);
  while (coord.running())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  server->Shutdown();
  server->Wait();
  return ok_void();
#else
  (void)coord;
  (void)listen_addr;
  (void)mtls_enabled;
  (void)cert;
  (void)key;
  (void)ca;
  return Error{"gRPC/protobuf support is not available in this build"};
#endif
}

}
}
