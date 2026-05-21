#include "reclaimnet/agent.h"
#include "reclaimnet/agent_service.h"
#include "reclaimnet/log.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/runtime_proto.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#if RECLAIMNET_HAVE_PROTO
#include <google/protobuf/util/json_util.h>
#include <grpcpp/create_channel.h>
#endif
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
namespace reclaimnet::agent {
namespace {
std::string host_of(const std::string &address) {
  auto pos = address.rfind(':');
  return pos == std::string::npos ? address : address.substr(0, pos);
}
std::string rate_key(const std::string &migration, uint32_t rank) {
  return migration + ":" + std::to_string(rank);
}
}
NodeAgent::NodeAgent(AgentConfig cfg, std::shared_ptr<GpuBackend> backend)
    : cfg_(std::move(cfg)), gpu_backend_(std::move(backend)),
      gpu_(gpu_backend_) {}
NodeAgent::~NodeAgent() { stop(); }
VoidResult NodeAgent::start() {
  if (auto valid = validate_agent_config(cfg_); !valid)
    return valid;
  runtime_ =
      std::make_unique<RuntimeManager>([this](const JobRuntimeSpec &spec) {
        return cfg_.gpu_isolation_enabled ? gpu_.release(spec.job_id, true)
                                          : ok_void();
      });
  checkpoints_ =
      std::make_unique<CheckpointManager>(cfg_.checkpoint_dir, *runtime_);
  MigrationTlsConfig tls;
  tls.cert_path = cfg_.tls_cert_path;
  tls.key_path = cfg_.tls_key_path;
  tls.ca_path = cfg_.tls_ca_path;
  migration_ = std::make_unique<MigrationClient>(tls);
  telemetry_ = std::make_unique<TelemetryCollector>(
      cfg_.iface, cfg_.telemetry_interval_s, gpu_backend_);
  coordinator_ = std::make_unique<CoordinatorClient>(
      cfg_.coordinator_address, cfg_.mtls_enabled, cfg_.tls_cert_path,
      cfg_.tls_key_path, cfg_.tls_ca_path);
  auto tel = telemetry_->sample_now();
  if (cfg_.gpu_isolation_enabled) {
    auto devices = gpu_.inventory();
    if (!devices)
      return devices.error();
    cfg_.total_vram_bytes = tel.total_vram_bytes;
  }
  if (auto registered = coordinator_->register_node(
          cfg_.node_id,
          cfg_.advertise_address.empty() ? cfg_.listen_address
                                         : cfg_.advertise_address,
          cfg_.building, cfg_.switch_id, cfg_.total_vram_bytes,
          cfg_.link_capacity_bps, std::max(1u, tel.gpu_count),
          tel.cuda_capability, cfg_.migration_port);
      !registered)
    return registered;
  if (cfg_.ebpf_enabled) {
    auto loaded = ebpf_load_tc_ratelimit(
        cfg_.bpf_object_dir + "/tc_ratelimit.bpf.o", cfg_.iface);
    if (!loaded)
      return loaded.error();
    ebpf_ = loaded.value();
    auto ports = ebpf_set_traffic_ports(
        ebpf_,
        static_cast<uint16_t>(std::stoul(
            cfg_.listen_address.substr(cfg_.listen_address.rfind(':') + 1))),
        cfg_.migration_port,
        static_cast<uint16_t>(std::stoul(cfg_.coordinator_address.substr(
            cfg_.coordinator_address.rfind(':') + 1))));
    if (!ports)
      return ports;
    auto kill = ebpf_load_kill_switch(
        cfg_.bpf_object_dir + "/kill_switch.bpf.o", cfg_.iface);
    if (!kill)
      return kill.error();
    reclaim_map_ = kill.value();
  }
#if RECLAIMNET_HAVE_PROTO
  receiver_ = std::make_unique<MigrationServer>(
      tls, *checkpoints_, MigrationReceiveProgressCb{},
      [this](const CheckpointMeta &meta, bool restored) -> VoidResult {
        proto::MigrationPreparation preparation;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          auto it = preparations_.find(meta.job_id);
          if (it == preparations_.end())
            return Error{"Migration was not prepared"};
          preparation = it->second;
        }
        if (meta.transfer_id != preparation.migration_id() ||
            meta.rank != preparation.rank())
          return Error{
              "Checkpoint commit does not match the prepared migration rank"};
        proto::CheckpointInfo info;
        info.set_job_id(meta.job_id);
        info.set_node_id(cfg_.node_id);
        info.set_replica_node_id(cfg_.node_id);
        info.set_sequence(meta.sequence);
        info.set_size_bytes(meta.size_bytes);
        info.set_completed_at(now_ns());
        info.set_path(meta.path);
        info.set_checksum_sha256(meta.sha256);
        info.set_checkpoint_id(meta.transfer_id);
        info.set_migration_id(meta.transfer_id);
        info.add_source_node_ids(preparation.source_node_id());
        info.set_rank(preparation.rank());
        info.set_gang_size(preparation.gang_size());
        info.set_restored(restored);
        if (restored) {
          std::lock_guard<std::mutex> lock(mutex_);
          active_jobs_.insert(meta.job_id);
        }
        return coordinator_->commit_checkpoint(info);
      },
      [this](const CheckpointMeta &meta, bool restoring,
             const std::string &peer) -> VoidResult {
        std::lock_guard<std::mutex> lock(mutex_);
        auto preparation = preparations_.find(meta.job_id);
        if (preparation == preparations_.end() ||
            preparation->second.migration_id() != meta.transfer_id ||
            preparation->second.rank() != meta.rank ||
            preparation->second.restore_after_transfer() != restoring ||
            preparation->second.source_node_id() != peer)
          return Error{"Migration manifest does not match the authenticated "
                       "prepared source"};
        return ok_void();
      });
  if (auto result =
          receiver_->start(cfg_.migration_port, host_of(cfg_.listen_address));
      !result)
    return result;
#endif
  service_ = std::make_unique<AgentServiceHost>(*this);
  if (auto result = service_->start(); !result)
    return result;
  running_ = true;
  cancel_ = false;
#if RECLAIMNET_HAVE_PROTO
  if (!cfg_.jobs_file.empty()) {
    std::ifstream input(cfg_.jobs_file);
    std::string line;
    while (std::getline(input, line)) {
      if (line.empty())
        continue;
      proto::RuntimeRegistration registration;
      auto parsed =
          google::protobuf::util::JsonStringToMessage(line, &registration);
      if (!parsed.ok()) {
        stop();
        return Error{parsed.ToString()};
      }
      if (auto result = register_runtime(registration); !result) {
        stop();
        return result;
      }
    }
  }
#endif
  control_thread_ = std::thread([this] { control_loop(); });
  return ok_void();
}
void NodeAgent::stop() {
  running_ = false;
  cancel_ = true;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &transfer : transfer_cancellations_)
      *transfer.second = true;
  }
  wake_.notify_all();
  if (control_thread_.joinable())
    control_thread_.join();
  if (service_)
    service_->stop();
  for (auto &worker : workers_)
    if (worker.joinable())
      worker.join();
  workers_.clear();
  if (receiver_)
    receiver_->stop();
  if (telemetry_)
    telemetry_->stop();
  if (cfg_.ebpf_enabled)
    ebpf_unload_all(cfg_.iface);
}
void NodeAgent::launch(const JobId &id, std::function<VoidResult()> operation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!in_flight_.insert(id).second)
    return;
  workers_.emplace_back([this, id, operation = std::move(operation)] {
    auto result = operation();
    if (!result)
      LOG_ERR("Job %s: %s", id.c_str(), result.error().message.c_str());
    std::lock_guard<std::mutex> lock(mutex_);
    in_flight_.erase(id);
    finished_workers_.insert(std::this_thread::get_id());
    wake_.notify_all();
  });
}
void NodeAgent::control_loop() {
  while (running_) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto worker = workers_.begin();
      while (worker != workers_.end()) {
        if (finished_workers_.erase(worker->get_id())) {
          worker->join();
          worker = workers_.erase(worker);
        } else
          ++worker;
      }
    }
    auto tel = telemetry_->sample_now();
    if (auto result = coordinator_->heartbeat(cfg_.node_id, tel); !result)
      LOG_ERR("Heartbeat: %s", result.error().message.c_str());
#if RECLAIMNET_HAVE_PROTO
    auto response = coordinator_->control_response();
    for (const auto &stop : response.stop_jobs()) {
      bool stopped;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped = stopped_barriers_.count(stop.barrier_id());
        auto transfer = transfer_cancellations_.find(stop.job_id());
        if (transfer != transfer_cancellations_.end())
          *transfer->second = true;
        pending_migrations_.erase(stop.job_id());
        plans_.erase(stop.job_id());
      }
      if (!stopped) {
        auto result = runtime_->stop(stop.job_id());
        if (result) {
          proto::SourceStopResult ack;
          ack.set_node_id(cfg_.node_id);
          ack.set_job_id(stop.job_id());
          ack.set_barrier_id(stop.barrier_id());
          ack.set_completed_ns(now_ns());
          result = coordinator_->source_stopped(ack);
        }
        if (result) {
          std::lock_guard<std::mutex> lock(mutex_);
          stopped_barriers_.insert(stop.barrier_id());
          active_jobs_.erase(stop.job_id());
        } else
          LOG_ERR("Gang source stop: %s", result.error().message.c_str());
      }
    }
    for (const auto &start : response.start_jobs()) {
      bool started;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        started = started_barriers_.count(start.barrier_id());
      }
      if (!started) {
        auto result = runtime_->start(start.job_id());
        if (result) {
          std::lock_guard<std::mutex> lock(mutex_);
          started_barriers_.insert(start.barrier_id());
        } else
          LOG_ERR("Gang start: %s", result.error().message.c_str());
      }
    }
    for (const auto &rate : response.rate_updates()) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto key = rate_key(rate.migration_id(), rate.rank());
      if (rate.allocation_version() < rate_versions_[key])
        continue;
      rate_versions_[key] = rate.allocation_version();
      rates_[key] = rate.allocated_bps();
      auto flow = active_flows_.find(key);
      if (cfg_.ebpf_enabled && flow != active_flows_.end())
        ebpf_set_flow_policy(ebpf_, flow->second, rate.allocated_bps(),
                             periodic_flows_[key]
                                 ? TrafficClass::P1_CHECKPOINT
                                 : TrafficClass::PLANNED_MIGRATION);
    }
    if (cfg_.ebpf_enabled) {
      auto stats = ebpf_read_flow_stats(ebpf_);
      double own_tx = 0, own_rx = 0;
      if (stats) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &flow : stats.value()) {
          bool own = flow.key.src_port == cfg_.migration_port ||
                     flow.key.dst_port == cfg_.migration_port;
          for (const auto &active : active_flows_)
            if (ebpf_flow_key(active.second) == ebpf_flow_key(flow.key))
              own = true;
          if (own) {
            own_tx += flow.tx_rate_bps;
            own_rx += flow.rx_rate_bps;
          }
        }
      }
      proto::TelemetryReport report;
      report.set_node_id(cfg_.node_id);
      report.set_timestamp(tel.sampled_ns);
      auto *tx = report.add_bw_samples();
      tx->set_node_id(cfg_.node_id);
      tx->set_src_node_id(cfg_.node_id);
      tx->set_dst_node_id(cfg_.switch_id);
      tx->set_bps(static_cast<uint64_t>(
          std::max(0.0, static_cast<double>(cfg_.link_capacity_bps) -
                            std::max(0.0, tel.link_tx_bps - own_tx))));
      tx->set_timestamp(tel.sampled_ns);
      auto *rx = report.add_bw_samples();
      rx->set_node_id(cfg_.node_id);
      rx->set_src_node_id(cfg_.switch_id);
      rx->set_dst_node_id(cfg_.node_id);
      rx->set_bps(static_cast<uint64_t>(
          std::max(0.0, static_cast<double>(cfg_.link_capacity_bps) -
                            std::max(0.0, tel.link_rx_bps - own_rx))));
      rx->set_timestamp(tel.sampled_ns);
      auto reported = coordinator_->report_telemetry(report);
      if (!reported)
        LOG_ERR("Capacity samples: %s", reported.error().message.c_str());
    }
    for (const auto &plan : response.checkpoint_intervals())
      apply_checkpoint(plan);
    for (const auto &recovery : response.recoveries()) {
      bool done;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        done = restored_checkpoints_.count(
            rate_key(recovery.checkpoint_id(), recovery.rank()));
      }
      if (!done)
        launch(recovery.job_id(),
               [this, recovery] { return restore(recovery); });
    }
    std::vector<proto::MigrationExecution> migrations;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto pending = pending_migrations_.begin();
      while (pending != pending_migrations_.end()) {
        if (!in_flight_.count(pending->first)) {
          migrations.push_back(pending->second);
          pending = pending_migrations_.erase(pending);
        } else
          ++pending;
      }
    }
    for (const auto &execution : migrations)
      launch(execution.job_id(), [this, execution] {
        auto size = runtime_->state_size(execution.job_id());
        if (!size)
          return VoidResult(size.error());
        double capture_cost = 0;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          capture_cost = capture_durations_[execution.job_id()];
        }
        double remaining =
            static_cast<double>(execution.rank().deadline_ns() - now_ns()) /
            1e9;
        double transfer_cost =
            bytes_to_bits(size.value()) / execution.rank().allocated_bps();
        if (remaining <
            capture_cost + transfer_cost + execution.rank().restore_time_s()) {
          proto::MigrationProgressRequest failure;
          failure.set_job_id(execution.job_id());
          failure.set_migration_id(execution.migration_id());
          failure.set_rank(execution.rank().rank());
          failure.set_failed(true);
          failure.set_timestamp_ns(now_ns());
          auto reported = coordinator_->migration_progress(failure);
          auto stopped = runtime_->stop(execution.job_id());
          return reported ? stopped : reported;
        }
        auto capture_start = now_ns();
        if (auto result = checkpoints_->take_checkpoint(
                execution.job_id(), size.value(), {}, true,
                execution.rank().deadline_ns());
            !result) {
          proto::MigrationProgressRequest failure;
          failure.set_job_id(execution.job_id());
          failure.set_migration_id(execution.migration_id());
          failure.set_rank(execution.rank().rank());
          failure.set_failed(true);
          failure.set_timestamp_ns(now_ns());
          coordinator_->migration_progress(failure);
          runtime_->stop(execution.job_id());
          return result;
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          capture_durations_[execution.job_id()] =
              static_cast<double>(now_ns() - capture_start) / 1e9;
        }
        auto meta = checkpoints_->latest_meta(execution.job_id());
        if (!meta)
          return VoidResult(meta.error());
        proto::FinalCheckpointRequest request;
        request.set_node_id(cfg_.node_id);
        request.set_job_id(execution.job_id());
        request.set_migration_id(execution.migration_id());
        request.set_rank(execution.rank().rank());
        request.set_size_bytes(meta.value().size_bytes);
        request.set_timestamp_ns(now_ns());
        auto prepared = coordinator_->prepare_final_checkpoint(request);
        if (!prepared) {
          runtime_->stop(execution.job_id());
          return VoidResult(prepared.error());
        }
        auto rank = execution.rank();
        rank.set_allocated_bps(prepared.value().allocated_bps());
        rank.set_eligible_start_ns(prepared.value().eligible_start_ns());
        rank.set_deadline_ns(prepared.value().deadline_ns());
        rank.set_allocation_version(prepared.value().allocation_version());
        return transfer(rank, execution.job_id(), execution.migration_id(),
                        true, rank.checkpoint_sequence());
      });
    std::vector<proto::CheckpointInterval> due;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (const auto &pair : plans_) {
        const auto &plan = pair.second;
        auto anchor = last_checkpoint_.find(pair.first);
        if (anchor == last_checkpoint_.end())
          last_checkpoint_[pair.first] = now_ns();
        else if (plan.feasible() && plan.interval_ns() > 0 &&
                 now_ns() >= plan.eligible_start_ns() &&
                 now_ns() - anchor->second >=
                     static_cast<int64_t>(plan.interval_ns()) &&
                 !in_flight_.count(pair.first) &&
                 !pending_migrations_.count(pair.first))
          due.push_back(plan);
      }
    }
    for (const auto &plan : due)
      launch(plan.job_id(), [this, plan] { return periodic_checkpoint(plan); });
#endif
    std::unique_lock<std::mutex> lock(mutex_);
    wake_.wait_for(lock, std::chrono::duration<double>(cfg_.control_poll_s),
                   [this] { return !running_; });
  }
}
#if RECLAIMNET_HAVE_PROTO
std::shared_ptr<grpc::Channel>
NodeAgent::target_channel(const std::string &address) const {
  auto credentials = insecure_channel_credentials();
  if (cfg_.mtls_enabled) {
    auto tls = load_tls_bundle(cfg_.tls_cert_path, cfg_.tls_key_path,
                               cfg_.tls_ca_path);
    if (!tls)
      return {};
    auto result = make_client_credentials(tls.value());
    if (!result)
      return {};
    credentials = result.value();
  }
  return grpc::CreateChannel(address, credentials);
}
VoidResult
NodeAgent::register_runtime(const proto::RuntimeRegistration &registration,
                            bool upstream, bool allocate_gpu, bool active) {
  auto spec = from_proto(registration.runtime());
  if (spec.job_id.empty())
    spec.job_id = registration.job().job_id();
  const auto ranks = std::max(1u, registration.rank_count());
  if (!spec.memory_limit_bytes)
    spec.memory_limit_bytes =
        registration.job().vram_bytes() / ranks +
        (registration.rank() < registration.job().vram_bytes() % ranks);
  if (cfg_.gpu_isolation_enabled && allocate_gpu) {
    auto devices = gpu_.inventory();
    if (!devices)
      return devices.error();
    uint32_t count = std::max(1u, registration.gpu_count_required());
    std::vector<std::string> assigned;
    uint64_t bytes = (spec.memory_limit_bytes + count - 1) / count;
    for (const auto &reservation : gpu_.reservations())
      if (reservation.job_id == spec.job_id) {
        if (reservation.vram_bytes != bytes)
          return Error{
              "GPU reservation conflicts with the active job allocation"};
        assigned.push_back(reservation.device_uuid);
      }
    if (!assigned.empty() && assigned.size() != count)
      return Error{"GPU count conflicts with the active job allocation"};
    for (const auto &device : devices.value()) {
      if (assigned.size() == count)
        break;
      if (device.cuda_capability < registration.cuda_capability_required())
        continue;
      if (device.free_vram_bytes < bytes)
        continue;
      auto reservation = gpu_.reserve(spec.job_id, device.index, bytes,
                                      device.mig_enabled ? -2 : -1);
      if (!reservation)
        continue;
      assigned.push_back(reservation.value().device_uuid);
    }
    if (assigned.size() != count) {
      gpu_.release(spec.job_id, false);
      return Error{"Insufficient GPU resources"};
    }
    spec.gpu_devices = "device=";
    for (size_t i = 0; i < assigned.size(); ++i) {
      if (i)
        spec.gpu_devices += ",";
      spec.gpu_devices += assigned[i];
    }
  }
  spec.seccomp_profile = cfg_.seccomp_profile;
  if (auto result = runtime_->register_job(spec); !result)
    return result;
  proto::RuntimeRegistration saved = registration;
  to_proto(spec, saved.mutable_runtime());
  if (active) {
    auto captured_at = now_ns();
    if (auto captured = checkpoints_->take_checkpoint(
            spec.job_id, registration.job().checkpoint_size(), {});
        !captured)
      return captured;
    auto meta = checkpoints_->latest_meta(spec.job_id);
    if (!meta)
      return meta.error();
    auto ranks = std::max(1u, registration.rank_count());
    saved.mutable_job()->set_checkpoint_size(meta.value().size_bytes * ranks);
    std::lock_guard<std::mutex> lock(mutex_);
    pending_checkpoints_[spec.job_id] = {meta.value().sequence, captured_at};
    capture_durations_[spec.job_id] =
        static_cast<double>(now_ns() - captured_at) / 1e9;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    registrations_[spec.job_id] = saved;
    if (active)
      active_jobs_.insert(spec.job_id);
    last_checkpoint_.try_emplace(spec.job_id, 0);
  }
  if (upstream && saved.rank() == 0)
    return coordinator_->register_runtime(saved, cfg_.node_id);
  return ok_void();
}
VoidResult
NodeAgent::prepare_migration(const proto::MigrationPreparation &request) {
  auto spec = from_proto(request.runtime());
  spec.defer_start = request.restore_after_transfer();
  auto state = std::filesystem::path(cfg_.checkpoint_dir) /
               ("state-" + spec.job_id + "-" + std::to_string(request.rank()));
  spec.state_path = state.string();
  if (!spec.container_name.empty())
    spec.container_name = spec.job_id + "-" + cfg_.node_id;
  proto::RuntimeRegistration registration;
  to_proto(spec, registration.mutable_runtime());
  registration.mutable_job()->set_job_id(spec.job_id);
  registration.mutable_job()->set_vram_bytes(
      request.vram_bytes() ? request.vram_bytes() : spec.memory_limit_bytes);
  registration.set_rank_count(request.gang_size());
  registration.set_rank(request.rank());
  registration.set_gpu_count_required(request.gpu_count_required());
  registration.set_cuda_capability_required(request.cuda_capability_required());
  if (auto result = register_runtime(registration, false,
                                     request.restore_after_transfer(), false);
      !result)
    return result;
  std::lock_guard<std::mutex> lock(mutex_);
  preparations_[spec.job_id] = request;
  return ok_void();
}
VoidResult
NodeAgent::prepare_target(const std::string &address,
                          const proto::MigrationPreparation &request) {
  auto channel = target_channel(address);
  if (!channel)
    return Error{"Target TLS credentials could not be loaded"};
  proto::Empty response;
  grpc::ClientContext context;
  coordinator_->authorize(context);
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));
  auto status = proto::NodeAgentService::NewStub(channel)->PrepareMigration(
      &context, request, &response);
  return status.ok() ? ok_void() : err_void(status.error_message());
}
void NodeAgent::configure_transfer(MigrationParams &params,
                                   const std::string &key, bool periodic,
                                   uint64_t version) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (version >= rate_versions_[key]) {
      rate_versions_[key] = version;
      rates_[key] = params.allocated_bps;
    } else
      params.allocated_bps = rates_.at(key);
  }
  params.rate_provider = [this, key] {
    std::lock_guard<std::mutex> lock(mutex_);
    return rates_.at(key);
  };
  params.flow_started = [this, key,
                         periodic](const FiveTuple &flow) -> VoidResult {
    std::lock_guard<std::mutex> lock(mutex_);
    active_flows_[key] = flow;
    periodic_flows_[key] = periodic;
    if (!cfg_.ebpf_enabled)
      return ok_void();
    return ebpf_set_flow_policy(ebpf_, flow, rates_.at(key),
                                periodic ? TrafficClass::P1_CHECKPOINT
                                         : TrafficClass::PLANNED_MIGRATION);
  };
  params.flow_finished = [this, key](const FiveTuple &flow) {
    if (cfg_.ebpf_enabled)
      ebpf_remove_flow(ebpf_, flow);
    std::lock_guard<std::mutex> lock(mutex_);
    active_flows_.erase(key);
    periodic_flows_.erase(key);
  };
}
VoidResult NodeAgent::transfer(const proto::RankMigration &rank,
                               const JobId &job, const std::string &id,
                               bool restoring, uint32_t sequence) {
  auto spec = runtime_->spec(job);
  if (!spec)
    return spec.error();
  proto::MigrationPreparation preparation;
  preparation.set_source_node_id(cfg_.node_id);
  to_proto(spec.value(), preparation.mutable_runtime());
  preparation.set_migration_id(id);
  preparation.set_rank(rank.rank());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto reg = registrations_.find(job);
    preparation.set_gang_size(reg == registrations_.end()
                                  ? 1
                                  : std::max(1u, reg->second.rank_count()));
    if (reg != registrations_.end()) {
      preparation.set_vram_bytes(spec.value().memory_limit_bytes);
      preparation.set_gpu_count_required(reg->second.gpu_count_required());
      preparation.set_cuda_capability_required(
          reg->second.cuda_capability_required());
    }
  }
  preparation.set_restore_after_transfer(restoring);
  if (auto result = prepare_target(rank.destination_address(), preparation);
      !result)
    return result;
  auto meta = checkpoints_->latest_meta(job);
  if (!meta)
    return meta.error();
  MigrationParams params;
  params.job_id = job;
  params.dst_host = host_of(rank.destination_address());
  params.dst_port = static_cast<uint16_t>(rank.destination_port());
  params.checkpoint_path = meta.value().path;
  params.checkpoint = meta.value();
  params.transfer_id = id;
  params.rank = rank.rank();
  if (sequence)
    params.checkpoint.sequence = sequence;
  params.allocated_bps = rank.allocated_bps();
  params.deadline_ns = rank.deadline_ns();
  params.deadline_s = static_cast<double>(params.deadline_ns - now_ns()) / 1e9;
  params.restore_after_transfer = restoring;
  auto key = rate_key(id, rank.rank());
  configure_transfer(params, key, !restoring, rank.allocation_version());
  auto cancellation = std::make_shared<std::atomic<bool>>(cancel_.load());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_jobs_.count(job))
      *cancellation = true;
    transfer_cancellations_[job] = cancellation;
  }
  if (rank.eligible_start_ns() > now_ns()) {
    std::unique_lock<std::mutex> lock(mutex_);
    wake_.wait_for(
        lock, std::chrono::nanoseconds(rank.eligible_start_ns() - now_ns()),
        [this, cancellation] {
          return cancel_.load() || cancellation->load();
        });
  }
  FiveTuple wire_flow{};
  uint64_t observed = 0, resume_offset = 0;
  bool first_progress = true;
  auto flow_started = params.flow_started;
  params.flow_started = [&](const FiveTuple &flow) {
    wire_flow = flow;
    return flow_started(flow);
  };
  int64_t transfer_start = now_ns();
  auto result = migration_->send(
      params,
      [this, job, id, rank, restoring, &observed, &resume_offset,
       &first_progress](uint64_t bytes) {
        observed = bytes;
        if (first_progress) {
          resume_offset = bytes;
          first_progress = false;
        }
        if (!restoring)
          return;
        proto::MigrationProgressRequest request;
        request.set_job_id(job);
        request.set_migration_id(id);
        request.set_rank(rank.rank());
        request.set_bytes_transferred(bytes);
        request.set_timestamp_ns(now_ns());
        auto result = coordinator_->migration_progress(request);
        if (!result)
          LOG_ERR("Migration progress: %s", result.error().message.c_str());
      },
      *cancellation);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    transfer_cancellations_.erase(job);
  }
#if RECLAIMNET_HAVE_JSON
  int64_t transfer_end = now_ns();
  nlohmann::json flow{
      {"src_ip", wire_flow.src_ip},
      {"dst_ip", wire_flow.dst_ip},
      {"src_port", wire_flow.src_port},
      {"dst_port", wire_flow.dst_port},
      {"protocol", wire_flow.protocol},
      {"flow_id", "flow-" + id + "-" + std::to_string(rank.rank())},
      {"flow_class", restoring ? "PLANNED_MIGRATION" : "P1_CHECKPOINT"},
      {"traffic_class", restoring ? "AF41" : "CS1"},
      {"checkpoint_id", id},
      {"migration_id",
       restoring ? nlohmann::json(id) : nlohmann::json(nullptr)},
      {"job_id", job},
      {"rank", rank.rank()},
      {"src_node_id", cfg_.node_id},
      {"dst_node_id", rank.destination_node_id()},
      {"metadata_only", false},
      {"bytes", observed - resume_offset},
      {"planned_bytes", meta.value().size_bytes},
      {"ts_ns", transfer_start},
      {"end_ns", transfer_end},
      {"average_rate_bps",
       transfer_end > transfer_start
           ? bytes_to_bits(observed - resume_offset) /
                 (static_cast<double>(transfer_end - transfer_start) / 1e9)
           : 0},
      {"status", result ? "COMPLETE" : "ABORTED"}};
  proto::TelemetryReport report;
  report.set_node_id(cfg_.node_id);
  report.set_timestamp(transfer_end);
  report.add_flow_records_json(flow.dump());
  auto exported = coordinator_->report_telemetry(report);
  if (!exported)
    LOG_ERR("Flow export: %s", exported.error().message.c_str());
#endif
  if (!restoring)
    return result;
  if (auto stopped = runtime_->stop(job); !stopped)
    return stopped;
  proto::MigrationProgressRequest request;
  request.set_job_id(job);
  request.set_migration_id(id);
  request.set_rank(rank.rank());
  request.set_bytes_transferred(result ? meta.value().size_bytes : 0);
  request.set_completed(static_cast<bool>(result));
  request.set_failed(!result);
  request.set_timestamp_ns(now_ns());
  auto reported = coordinator_->migration_progress(request);
  if (!result) {
    return result;
  }
  if (!reported)
    return reported;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    active_jobs_.erase(job);
  }
  return checkpoints_->purge(job);
}
VoidResult
NodeAgent::periodic_checkpoint(const proto::CheckpointInterval &previous) {
  int64_t capture_start = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto pending = pending_checkpoints_.find(previous.job_id());
    if (pending != pending_checkpoints_.end())
      capture_start = pending->second.second;
  }
  if (!capture_start) {
    auto size = runtime_->state_size(previous.job_id());
    if (!size)
      return size.error();
    capture_start = now_ns();
    if (auto captured = checkpoints_->take_checkpoint(
            previous.job_id(), size.value(), {}, false,
            capture_start + static_cast<int64_t>(previous.interval_ns()));
        !captured)
      return captured;
    auto captured = checkpoints_->latest_meta(previous.job_id());
    if (!captured)
      return captured.error();
    std::lock_guard<std::mutex> lock(mutex_);
    capture_durations_[previous.job_id()] =
        static_cast<double>(now_ns() - capture_start) / 1e9;
    pending_checkpoints_[previous.job_id()] = {captured.value().sequence,
                                               capture_start};
  }
  auto meta = checkpoints_->latest_meta(previous.job_id());
  if (!meta)
    return meta.error();
  proto::CheckpointRequest request;
  request.set_node_id(cfg_.node_id);
  request.set_job_id(previous.job_id());
  request.set_checkpoint_size_bytes(meta.value().size_bytes);
  request.set_timestamp_ns(now_ns());
  request.set_rank(previous.rank());
  auto plan = coordinator_->begin_checkpoint(request);
  if (!plan)
    return plan.error();
  proto::RankMigration rank;
  rank.set_rank(plan.value().rank());
  rank.set_source_node_id(cfg_.node_id);
  rank.set_destination_node_id(plan.value().replica_node_id());
  rank.set_destination_address(plan.value().replica_address());
  rank.set_destination_port(plan.value().replica_port());
  rank.set_allocated_bps(plan.value().exact_allocated_bps());
  rank.set_eligible_start_ns(plan.value().eligible_start_ns());
  rank.set_deadline_ns(plan.value().deadline_ns());
  rank.set_allocation_version(plan.value().allocation_version());
  auto result = transfer(rank, previous.job_id(), plan.value().checkpoint_id(),
                         false, plan.value().checkpoint_sequence());
  if (result) {
    std::lock_guard<std::mutex> lock(mutex_);
    last_checkpoint_[previous.job_id()] = capture_start;
    pending_checkpoints_.erase(previous.job_id());
  }
  return result;
}
void NodeAgent::apply_checkpoint(const proto::CheckpointInterval &plan) {
  std::lock_guard<std::mutex> lock(mutex_);
  plans_[plan.job_id()] = plan;
}
VoidResult NodeAgent::restore(const proto::RecoveryPlan &plan) {
  auto hash = sha256_file(plan.path());
  if (!hash)
    return hash.error();
  if (hash.value() != plan.checksum_sha256())
    return Error{"Recovery checksum does not match committed checkpoint"};
  proto::MigrationPreparation preparation;
  *preparation.mutable_runtime() = plan.runtime();
  preparation.set_rank(plan.rank());
  preparation.set_gang_size(plan.gang_size());
  preparation.set_migration_id(plan.checkpoint_id());
  preparation.set_restore_after_transfer(true);
  preparation.set_vram_bytes(plan.vram_bytes());
  preparation.set_gpu_count_required(plan.gpu_count_required());
  preparation.set_cuda_capability_required(plan.cuda_capability_required());
  if (auto result = prepare_migration(preparation); !result)
    return result;
  if (auto result = runtime_->restore(plan.job_id(), plan.path()); !result)
    return result;
  proto::RecoveryResult result;
  result.set_node_id(cfg_.node_id);
  result.set_job_id(plan.job_id());
  result.set_checkpoint_id(plan.checkpoint_id());
  result.set_rank(plan.rank());
  result.set_restored(true);
  result.set_completed_ns(now_ns());
  auto reported = coordinator_->recovery_complete(result);
  if (reported) {
    std::lock_guard<std::mutex> lock(mutex_);
    restored_checkpoints_.insert(rate_key(plan.checkpoint_id(), plan.rank()));
    active_jobs_.insert(plan.job_id());
  }
  return reported;
}
VoidResult
NodeAgent::execute_migration(const proto::MigrationExecution &execution) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!accepted_migrations_
           .insert(execution.job_id() + ":" +
                   rate_key(execution.migration_id(), execution.rank().rank()))
           .second)
    return ok_void();
  pending_migrations_[execution.job_id()] = execution;
  plans_.erase(execution.job_id());
  pending_checkpoints_.erase(execution.job_id());
  auto active = transfer_cancellations_.find(execution.job_id());
  if (active != transfer_cancellations_.end())
    *active->second = true;
  wake_.notify_all();
  return ok_void();
}

VoidResult NodeAgent::apply_kill(const proto::KillSwitchCommand &command) {
  if (!cfg_.ebpf_enabled)
    return Error{"Kernel data plane is not enabled"};
  int64_t grace = command.grace_deadline_ns() > now_ns()
                      ? mono_ns() + command.grace_deadline_ns() - now_ns()
                      : 0;
  return ebpf_set_reclaim_policy(
      reclaim_map_, command.activate(), command.allow_existing(), grace,
      static_cast<uint16_t>(std::stoul(
          cfg_.listen_address.substr(cfg_.listen_address.rfind(':') + 1))),
      cfg_.migration_port);
}
#endif
#if RECLAIMNET_HAVE_PROTO
VoidResult
NodeAgent::dispatch_reclaim(const ReclaimNotification &notification,
                            const proto::ReclaimSignalResponse &response) {
  if (!response.success())
    return Error{response.error_message()};
  if (response.recovery()) {
    auto result = runtime_->stop(notification.job_id);
    if (result) {
      std::lock_guard<std::mutex> lock(mutex_);
      active_jobs_.erase(notification.job_id);
    }
    return result;
  }
  for (const auto &rank : response.ranks()) {
    proto::MigrationExecution execution;
    execution.set_job_id(notification.job_id);
    execution.set_migration_id(response.migration_id());
    *execution.mutable_rank() = rank;
    execution.set_notice_s(notification.deadline_s);
    if (rank.source_node_id() == cfg_.node_id) {
      execute_migration(execution);
      continue;
    }
    auto channel = target_channel(rank.source_address());
    if (!channel)
      return Error{"Rank source channel failed"};
    proto::Empty result;
    grpc::ClientContext context;
    coordinator_->authorize(context);
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    auto status = proto::NodeAgentService::NewStub(channel)->ExecuteMigration(
        &context, execution, &result);
    if (!status.ok())
      return Error{status.error_message()};
  }
  return ok_void();
}
#endif
VoidResult NodeAgent::handle_reclaim(const ReclaimNotification &input) {
  auto notification = input;
  if (notification.event_timestamp_ns <= 0)
    notification.event_timestamp_ns = now_ns();
#if RECLAIMNET_HAVE_PROTO
  std::vector<ReclaimNotification> notices;
  if (notification.job_id.empty()) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &job : active_jobs_) {
      auto n = notification;
      n.job_id = job;
      notices.push_back(n);
    }
  } else {
    notices.push_back(notification);
  }
  VoidResult readiness = ok_void();
  if (cfg_.ebpf_enabled) {
    proto::KillSwitchCommand command;
    command.set_activate(true);
    command.set_allow_existing(notification.type == ReclaimType::SCHEDULED);
    command.set_grace_deadline_ns(
        (notification.event_timestamp_ns > 0 ? notification.event_timestamp_ns
                                             : now_ns()) +
        static_cast<int64_t>(notification.deadline_s * 1e9));
    readiness = apply_kill(command);
  }
  if (notification.type == ReclaimType::EMERGENCY) {
    for (const auto &n : notices) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transfer = transfer_cancellations_.find(n.job_id);
        if (transfer != transfer_cancellations_.end())
          *transfer->second = true;
        plans_.erase(n.job_id);
        pending_migrations_.erase(n.job_id);
        active_jobs_.erase(n.job_id);
      }
      if (auto stopped = runtime_->stop(n.job_id); !stopped)
        readiness = stopped;
    }
  }
  if (!readiness)
    return readiness;
  if (notification.job_id.empty()) {
    proto::BatchMigrationRequest request;
    for (const auto &n : notices) {
      auto *req = request.add_requests();
      req->set_node_id(cfg_.node_id);
      req->set_job_id(n.job_id);
      req->set_deadline_s(n.deadline_s);
      req->set_event_timestamp_ns(n.event_timestamp_ns);
      req->set_reclaim_type(n.type == ReclaimType::EMERGENCY
                                ? proto::RECLAIM_EMERGENCY
                                : proto::RECLAIM_SCHEDULED);
    }
    auto response = coordinator_->migration_batch(request);
    if (!response)
      return response.error();
    VoidResult result = ok_void();
    for (size_t i = 0; i < notices.size(); ++i) {
      auto deployed = dispatch_reclaim(
          notices[i], response.value().decisions(static_cast<int>(i)));
      if (!deployed)
        result = deployed;
    }
    return result;
  }
  auto response = coordinator_->migration_decision(notification, cfg_.node_id);
  if (!response)
    return response.error();
  return dispatch_reclaim(notification, response.value());
#else
  (void)notification;
  return Error{"gRPC transport is required for reclaim"};
#endif
}
}
