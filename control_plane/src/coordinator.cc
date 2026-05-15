#include "reclaimnet/coordinator.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"
#include "reclaimnet/trace.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif

namespace reclaimnet::control {
namespace {
std::vector<NodeId> sources_of(const JobRecord &job) {
  return job.rank_nodes.empty() ? std::vector<NodeId>{job.node_id}
                                : job.rank_nodes;
}
uint64_t rank_bytes(const JobRecord &job, uint32_t rank = 0) {
  const auto count = std::max<uint32_t>(1, job.rank_count);
  if (job.rank_checkpoint_sizes.size() == count && rank < count)
    return job.rank_checkpoint_sizes[rank];
  return job.checkpoint_size / count + (rank < job.checkpoint_size % count);
}
uint64_t rank_vram(const JobRecord &job, uint32_t rank = 0) {
  const auto count = std::max<uint32_t>(1, job.rank_count);
  return job.vram_bytes / count + (rank < job.vram_bytes % count);
}
double rank_share(const JobRecord &job, uint32_t rank) {
  return job.checkpoint_size > 0
             ? static_cast<double>(rank_bytes(job, rank)) / job.checkpoint_size
             : 1.0 / std::max<uint32_t>(1, job.rank_count);
}
std::string state_name(JobState state) {
  switch (state) {
  case JobState::RUNNING:
    return "RUNNING";
  case JobState::MIGRATING:
    return "MIGRATING";
  case JobState::SUSPENDED:
    return "SUSPENDED";
  case JobState::FAILED:
    return "FAILED";
  case JobState::COMPLETED:
    return "COMPLETED";
  case JobState::CHECKPOINTING:
    return "CHECKPOINTING";
  default:
    return "UNKNOWN";
  }
}
#if RECLAIMNET_HAVE_JSON
using J = nlohmann::json;
J checkpoint_json(const ReplicaCheckpoint &checkpoint) {
  return {{"job_id", checkpoint.job_id},
          {"checkpoint_id", checkpoint.checkpoint_id},
          {"sequence", checkpoint.sequence},
          {"size_bytes", checkpoint.size_bytes},
          {"completed_at_ns", checkpoint.completed_at_ns},
          {"source_node_ids", checkpoint.source_node_ids},
          {"replica_node_id", checkpoint.replica_node_id},
          {"path", checkpoint.path},
          {"checksum_sha256", checkpoint.checksum_sha256},
          {"node_incarnation", checkpoint.node_incarnation},
          {"rank", checkpoint.rank},
          {"gang_size", checkpoint.gang_size}};
}
ReplicaCheckpoint checkpoint_value(const J &value) {
  ReplicaCheckpoint checkpoint;
  checkpoint.job_id = value.at("job_id");
  checkpoint.checkpoint_id = value.at("checkpoint_id");
  checkpoint.sequence = value.at("sequence");
  checkpoint.size_bytes = value.at("size_bytes");
  checkpoint.completed_at_ns = value.at("completed_at_ns");
  checkpoint.source_node_ids =
      value.at("source_node_ids").get<std::vector<NodeId>>();
  checkpoint.replica_node_id = value.at("replica_node_id");
  checkpoint.path = value.at("path");
  checkpoint.checksum_sha256 = value.at("checksum_sha256");
  checkpoint.node_incarnation = value.value("node_incarnation", uint64_t{0});
  checkpoint.rank = value.value("rank", 0u);
  checkpoint.gang_size = value.value("gang_size", 1u);
  return checkpoint;
}
#endif
}

Coordinator::Coordinator(CoordinatorConfig cfg)
    : cfg_(std::move(cfg)), hazard_(cfg_.hazard_window_s),
      capacities_(cfg_.capacity_window_s, cfg_.capacity_confidence_factor) {}
Coordinator::~Coordinator() { stop(); }
void Coordinator::trace_locked(const std::string &file,
                               const std::string &record) {
  if (writer_)
    if (auto written = writer_->append(file, record); !written)
      LOG_ERR("Trace append failed: %s", written.error().message.c_str());
}
void Coordinator::persist_job_locked(const JobId &id) {
  if (auto job = job_reg_.get(id))
    if (auto saved = store_->upsert_job(*job); !saved)
      LOG_ERR("Job persistence failed: %s", saved.error().message.c_str());
}
VoidResult Coordinator::start(bool background) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (running_)
    return err_void("Coordinator is already running");
  if (auto valid = validate_coordinator_config(cfg_); !valid)
    return valid;
  topo_ = std::make_unique<TopologyGraph>();
  if (!cfg_.topology_file.empty()) {
    auto topology = TopologyGraph::load_from_file(cfg_.topology_file);
    if (!topology)
      return topology.error();
    *topo_ = std::move(topology.value());
  }
  store_ = std::make_unique<PersistenceStore>(cfg_.sqlite_path);
  if (auto schema = store_->init_schema(); !schema)
    return schema;
  p3_ = std::make_unique<P3Admission>(cfg_.migration_share,
                                      cfg_.research_min_bps);
  job_reg_.clear();
  node_reg_.clear();
  checkpoint_plans_.clear();
  pending_rates_.clear();
  migration_destinations_.clear();
  migration_sessions_.clear();
  recovery_queue_.clear();
  runtime_specs_.clear();
  recovery_barriers_.clear();
  stop_queue_.clear();
  stopped_sources_.clear();
  recovery_acks_.clear();
  resume_queue_.clear();
  completed_ranks_.clear();
  restored_ranks_.clear();
  committed_ranks_.clear();
  checkpoint_sequence_ids_.clear();
  checkpoint_sizes_.clear();
  pending_invalidations_.clear();
  auto jobs = store_->load_all_jobs();
  if (!jobs)
    return jobs.error();
  for (auto job : jobs.value()) {
    if (job.state == JobState::MIGRATING) {
      job.state = JobState::SUSPENDED;
      job.migration_dst.clear();
      store_->upsert_job(job);
    }
    job_reg_.add(job);
  }
  auto nodes = store_->load_all_nodes();
  if (!nodes)
    return nodes.error();
  for (const auto &node : nodes.value()) {
    node_reg_.upsert(node);
    if (node.registered_at_ns > 0)
      hazard_.observe_node(node.node_id, node.registered_at_ns);
    p3_->set_destination_resources(node.node_id, node.telemetry.vram_free_bytes,
                                   node.gpu_free_count);
  }
  auto runtimes = store_->load_runtime("runtime_spec");
  if (!runtimes)
    return runtimes.error();
  for (const auto &value : runtimes.value())
    runtime_specs_[value.first] = value.second;
  auto ports = store_->load_runtime("migration_port");
  if (!ports)
    return ports.error();
  for (const auto &value : ports.value()) {
    try {
      migration_ports_[value.first] =
          static_cast<uint16_t>(std::stoul(value.second));
    } catch (const std::exception &) {
      return err_void("Invalid persisted migration port");
    }
  }
#if RECLAIMNET_HAVE_JSON
  auto checkpoints = store_->load_runtime("checkpoint");
  if (!checkpoints)
    return checkpoints.error();
  try {
    for (const auto &value : checkpoints.value()) {
      auto checkpoint = checkpoint_value(J::parse(value.second));
      if (auto committed = catalog_.commit(checkpoint); !committed)
        return committed;
      checkpoint_sequences_[checkpoint.job_id] = std::max(
          checkpoint_sequences_[checkpoint.job_id], checkpoint.sequence);
    }
    auto invalidations = store_->load_runtime("invalidation");
    if (!invalidations)
      return invalidations.error();
    for (const auto &value : invalidations.value())
      catalog_.invalidate_node(value.first, std::stoll(value.second));
    auto deadlines = store_->load_runtime("pending_reclaim_deadline");
    if (!deadlines)
      return deadlines.error();
    for (const auto &record : deadlines.value())
      pending_invalidations_[record.first] = std::stoll(record.second);
    auto targets = store_->load_runtime("migration_target");
    if (!targets)
      return targets.error();
    for (const auto &record : targets.value()) {
      const auto value = J::parse(record.second);
      migration_destinations_[record.first] =
          value.at("destinations").get<std::vector<NodeId>>();
      migration_sessions_[record.first] = value.at("migration_id");
    }
    auto restored = store_->load_runtime("restored_rank");
    if (!restored)
      return restored.error();
    for (const auto &record : restored.value()) {
      const auto value = J::parse(record.second);
      restored_ranks_[value.at("migration_id").get<std::string>()].insert(
          value.at("rank").get<uint32_t>());
    }
    auto resumes = store_->load_runtime("resume_barrier");
    if (!resumes)
      return resumes.error();
    for (const auto &record : resumes.value()) {
      const auto value = J::parse(record.second);
      const auto job = job_reg_.get(record.first);
      if (job && job->state == JobState::RUNNING)
        for (const auto &node : value.at("destinations"))
          resume_queue_[node.get<NodeId>()].emplace_back(
              record.first, value.at("barrier_id").get<std::string>());
    }
    auto acknowledgements = store_->load_runtime("completed_migration");
    if (!acknowledgements)
      return acknowledgements.error();
    for (const auto &record : acknowledgements.value())
      completed_ranks_[record.first] =
          J::parse(record.second).at("ranks").get<std::set<uint32_t>>();
    auto recoveries = store_->load_runtime("recovery_ack");
    if (!recoveries)
      return recoveries.error();
    for (const auto &record : recoveries.value()) {
      const auto value = J::parse(record.second);
      recovery_acks_.emplace(value.at("node_id"), value.at("job_id"),
                             value.at("checkpoint_id"), value.at("rank"));
      if (value.contains("sequence"))
        completed_ranks_["recovery:" + value.at("job_id").get<std::string>() +
                         ":" +
                         std::to_string(value.at("sequence").get<uint32_t>())]
            .insert(value.at("rank").get<uint32_t>());
    }
  } catch (const std::exception &error) {
    return Error{error.what()};
  }
#endif
  p1_ = std::make_unique<P1Allocator>(cfg_.B_ckpt_bps);
  p2_ = std::make_unique<P2Selector>(cfg_.alpha, cfg_.beta, cfg_.gamma, *topo_,
                                     hazard_, node_reg_, &capacities_);
#if RECLAIMNET_HAVE_JSON
  try {
    auto samples = store_->load_runtime("hazard_sample");
    if (!samples)
      return samples.error();
    for (const auto &record : samples.value()) {
      const auto value = J::parse(record.second);
      if (auto observed = hazard_.observe_sample(
              value.at("node_id"), value.at("lambda_e"), value.at("timestamp"));
          !observed)
        return observed;
    }
    auto events = store_->load_runtime("hazard_event");
    if (!events)
      return events.error();
    for (const auto &record : events.value()) {
      const auto value = J::parse(record.second);
      hazard_.record_event(
          {value.at("node_id"), value.at("timestamp"), ReclaimType::EMERGENCY});
    }
    auto costs = store_->load_runtime("migration_cost");
    if (!costs)
      return costs.error();
    for (const auto &record : costs.value()) {
      const auto value = J::parse(record.second);
      if (auto observed = p2_->observe_migration_cost(value.at("node_id"),
                                                      value.at("duration_s"),
                                                      value.at("completed_ns"));
          !observed)
        return observed;
    }
  } catch (const std::exception &error) {
    return Error{error.what()};
  }
#endif
  for (const auto &node : topo_->all_nodes())
    for (const auto &link : topo_->links_from(node))
      p3_->add_link(link.src, link.dst, link.bandwidth_bps);
  if (!cfg_.trace_dir.empty()) {
    writer_ = std::make_unique<trace::Writer>(cfg_.trace_dir, cfg_.run_id);
    if (auto initialized = writer_->initialize(
            cfg_.checkpoint_beta, cfg_.checkpoint_loss_budget_s,
            cfg_.B_ckpt_bps, 1 - cfg_.migration_share, cfg_.research_min_bps,
            cfg_.epoch_seconds, cfg_.alpha, cfg_.p2_k_min,
            cfg_.p2_load_limit_pct, cfg_.minimum_useful_transfer_s,
            cfg_.capacity_confidence_factor);
        !initialized)
      return initialized;
  }
  running_ = true;
  snapshot_nodes_locked();
  if (background)
    epoch_thread_ = std::thread([this] { run_epoch_loop(); });
  return ok_void();
}
void Coordinator::stop() {
  if (!running_.exchange(false))
    return;
  epoch_cv_.notify_all();
  if (epoch_thread_.joinable())
    epoch_thread_.join();
  std::lock_guard<std::mutex> lock(control_mu_);
  if (writer_)
    if (auto result = writer_->finalize(); !result)
      LOG_ERR("Trace finalize: %s", result.error().message.c_str());
}
void Coordinator::run_epoch_loop() {
  while (running_) {
    run_one_epoch();
    std::unique_lock<std::mutex> lock(epoch_wait_mu_);
    epoch_cv_.wait_for(lock, std::chrono::duration<double>(cfg_.epoch_seconds),
                       [this] { return !running_; });
  }
}
void Coordinator::snapshot_nodes_locked() {
#if RECLAIMNET_HAVE_JSON
  if (!writer_)
    return;
  J nodes = J::array();
  for (const auto &node : node_reg_.all()) {
    const char *state = node.state == NodeState::ONLINE     ? "ONLINE"
                        : node.state == NodeState::DRAINING ? "DRAINING"
                                                            : "OFFLINE";
    nodes.push_back({{"node_id", node.node_id},
                     {"state", state},
                     {"address", node.address},
                     {"building", node.building},
                     {"switch_id", node.switch_id},
                     {"total_vram_bytes", node.total_vram_bytes},
                     {"link_capacity_bps", node.link_capacity_bps},
                     {"registered_at_ns", node.registered_at_ns},
                     {"last_heartbeat_ns", node.last_heartbeat_ns},
                     {"gpu_count", node.gpu_count},
                     {"cuda_capability", node.cuda_capability}});
  }
  if (auto result = writer_->snapshot("nodes.json", nodes.dump()); !result)
    LOG_ERR("Node trace snapshot failed: %s", result.error().message.c_str());
#endif
}
VoidResult Coordinator::register_node(const NodeRecord &request,
                                      uint16_t port) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  if (request.node_id.empty() || port == 0 ||
      !std::isfinite(request.cuda_capability) || request.cuda_capability < 0)
    return err_void("Invalid node registration");
  auto node = request;
  if (node.registered_at_ns <= 0)
    node.registered_at_ns = now_ns();
  if (auto previous = node_reg_.get(node.node_id);
      previous && node.registered_at_ns > previous->registered_at_ns) {
    catalog_.invalidate_node(node.node_id, node.registered_at_ns - 1);
    store_->set_runtime("invalidation", node.node_id,
                        std::to_string(node.registered_at_ns - 1));
    pending_invalidations_.erase(node.node_id);
    store_->erase_runtime("pending_reclaim_deadline", node.node_id);
  }
  if (node.last_heartbeat_ns <= 0)
    node.last_heartbeat_ns = node.registered_at_ns;
  if (auto saved = store_->upsert_node(node); !saved)
    return saved;
  if (auto saved = store_->set_runtime("migration_port", node.node_id,
                                       std::to_string(port));
      !saved)
    return saved;
  migration_ports_[node.node_id] = port;
  node_reg_.upsert(node);
  hazard_.observe_node(node.node_id, node.registered_at_ns);
  p3_->set_destination_resources(node.node_id, node.telemetry.vram_free_bytes,
                                 node.gpu_free_count);
  snapshot_nodes_locked();
  return ok_void();
}
VoidResult Coordinator::register_job(const JobRecord &request,
                                     std::string runtime) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  if (request.job_id.empty() || request.node_id.empty() ||
      !std::isfinite(request.lambda_i) || request.lambda_i < 0 ||
      request.rank_count == 0 || !std::isfinite(request.remaining_runtime_s) ||
      request.remaining_runtime_s < 0 ||
      !std::isfinite(request.write_cap_bps) || request.write_cap_bps < 0)
    return err_void("Invalid job registration");
  if (job_reg_.get(request.job_id))
    return err_void("Job already registered: " + request.job_id);
  auto job = request;
  if (job.rank_nodes.empty())
    job.rank_nodes = {job.node_id};
  if (job.rank_nodes.size() != job.rank_count)
    return err_void("Rank count does not match source placement");
  if (!job.rank_checkpoint_sizes.empty()) {
    if (job.rank_checkpoint_sizes.size() != job.rank_count)
      return err_void("Rank payload count does not match source placement");
    uint64_t total = 0;
    for (uint64_t bytes : job.rank_checkpoint_sizes) {
      if (bytes > std::numeric_limits<uint64_t>::max() - total)
        return err_void("Logical checkpoint payload overflow");
      total += bytes;
    }
    if (total != job.checkpoint_size)
      return err_void(
          "Rank payloads do not sum to the logical checkpoint size");
  }
  for (const auto &node : job.rank_nodes)
    if (!node_reg_.get(node))
      return err_void("Unknown rank source: " + node);
  if (job.submit_time_ns <= 0)
    job.submit_time_ns = now_ns();
  if (job.last_update_ns <= 0)
    job.last_update_ns = job.submit_time_ns;
  if (auto saved = store_->upsert_job(job); !saved)
    return saved;
  if (auto saved = store_->set_runtime("runtime_spec", job.job_id, runtime);
      !saved)
    return saved;
  runtime_specs_[job.job_id] = std::move(runtime);
  job_reg_.add(job);
#if RECLAIMNET_HAVE_JSON
  trace_locked("jobs.jsonl",
               J{{"job_id", job.job_id},
                 {"state", state_name(job.state)},
                 {"node_ids", job.rank_nodes},
                 {"logical_checkpoint_size_bytes", job.checkpoint_size},
                 {"per_rank_checkpoint_size_bytes", rank_bytes(job)},
                 {"logical_vram_bytes", job.vram_bytes},
                 {"lambda_i", job.lambda_i},
                 {"loss_budget_s", job.loss_budget_s > 0
                                       ? job.loss_budget_s
                                       : cfg_.checkpoint_loss_budget_s},
                 {"submit_time_ns", job.submit_time_ns},
                 {"last_update_ns", job.last_update_ns}}
                   .dump());
#endif
  return ok_void();
}
VoidResult Coordinator::on_hazard_sample(const NodeId &node, double rate,
                                         int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  auto result = hazard_.observe_sample(node, rate, stamp);
#if RECLAIMNET_HAVE_JSON
  if (result) {
    const auto sample =
        J{{"node_id", node}, {"lambda_e", rate}, {"timestamp", stamp}}.dump();
    if (auto saved = store_->set_runtime(
            "hazard_sample", node + ":" + std::to_string(stamp), sample);
        !saved)
      return saved;
    trace_locked("hazard_samples.jsonl", sample);
  }
#endif
  return result;
}
VoidResult Coordinator::on_bandwidth_sample(const NodeId &src,
                                            const NodeId &dst, double bps,
                                            int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  auto result = capacities_.observe_sample(src, dst, bps, stamp);
#if RECLAIMNET_HAVE_JSON
  if (result)
    trace_locked("bandwidth_samples.jsonl",
                 J{{"node_id", src},
                   {"src_node_id", src},
                   {"dst_node_id", dst},
                   {"bps", static_cast<uint64_t>(bps)},
                   {"timestamp", stamp}}
                     .dump());
#endif
  return result;
}
void Coordinator::on_heartbeat(const NodeId &id,
                               const NodeTelemetry &telemetry) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return;
  auto node = node_reg_.get(id);
  if (!node)
    return;
  const int64_t stamp =
      telemetry.updated_ns > 0 ? telemetry.updated_ns : now_ns();
  if (stamp < node->last_heartbeat_ns)
    return;
  node->telemetry = telemetry;
  node->last_heartbeat_ns = stamp;
  if (node->state == NodeState::UNKNOWN || node->state == NodeState::DEGRADED)
    node->state = NodeState::ONLINE;
  node->gpu_free_count = std::min(node->gpu_count, telemetry.gpu_free_count);
  if (telemetry.cuda_capability > 0)
    node->cuda_capability = telemetry.cuda_capability;
  node_reg_.upsert(*node);
  store_->upsert_node(*node);
  for (const auto &available : destination_capacity_locked())
    p3_->set_destination_resources(available.first, available.second.first,
                                   available.second.second);
#if RECLAIMNET_HAVE_JSON
  trace_locked("telemetry.jsonl",
               J{{"node_id", id},
                 {"gpu_util_pct", telemetry.gpu_util_pct},
                 {"vram_used_pct", telemetry.vram_used_pct},
                 {"vram_free_bytes", telemetry.vram_free_bytes},
                 {"link_tx_bps",
                  static_cast<uint64_t>(std::max(0.0, telemetry.link_tx_bps))},
                 {"link_rx_bps",
                  static_cast<uint64_t>(std::max(0.0, telemetry.link_rx_bps))},
                 {"cpu_load_1m", telemetry.cpu_load_1m},
                 {"timestamp", stamp}}
                   .dump());
#endif
}
std::unordered_map<NodeId, std::pair<uint64_t, uint32_t>>
Coordinator::destination_capacity_locked() const {
  std::unordered_map<NodeId, std::pair<uint64_t, uint32_t>> capacities;
  const auto checkpoints = catalog_.all();
  for (const auto &node : node_reg_.all())
    capacities[node.node_id] = {
        std::min(node.total_vram_bytes, node.telemetry.vram_free_bytes),
        std::min(node.gpu_count, node.gpu_free_count)};
  for (const auto &active : p3_->active_migrations()) {
    auto restored = restored_ranks_.find(active.migration_id);
    if (restored == restored_ranks_.end() ||
        !restored->second.count(active.rank))
      continue;
    auto node = node_reg_.get(active.dst);
    if (!node)
      continue;
    auto checkpoint = std::find_if(
        checkpoints.begin(), checkpoints.end(), [&](const auto &copy) {
          return copy.job_id == active.job_id &&
                 copy.checkpoint_id == active.migration_id &&
                 copy.rank == active.rank;
        });
    if (checkpoint == checkpoints.end() ||
        node->telemetry.updated_ns < checkpoint->completed_at_ns)
      continue;
    auto &capacity = capacities[active.dst];
    capacity.first += std::min(active.vram_required_bytes,
                               node->total_vram_bytes - capacity.first);
    capacity.second +=
        std::min(active.gpu_count_required, node->gpu_count - capacity.second);
  }
  return capacities;
}
void Coordinator::allocate_checkpoints_locked(int64_t stamp) {
  const auto jobs = job_reg_.by_state(JobState::RUNNING);
  std::vector<P1JobInput> inputs;
  std::vector<std::map<std::string, long double>> edge_counts;
  std::map<std::string, double> edge_capacity;
  std::unordered_map<JobId, std::vector<NodeId>> targets;
  long double weight_sum = 0;
  for (const auto &job : jobs) {
    P1JobInput input;
    input.job_id = job.job_id;
    input.checkpoint_size_b = job.checkpoint_size;
    input.write_cap_bps = job.write_cap_bps;
    input.checkpoint_beta = cfg_.checkpoint_beta;
    input.loss_budget_s = job.loss_budget_s > 0 ? job.loss_budget_s
                                                : cfg_.checkpoint_loss_budget_s;
    const auto sources = sources_of(job);
    const std::set<NodeId> hazard_nodes(sources.begin(), sources.end());
    input.lambda_i = 0;
    for (const auto &node : hazard_nodes) {
      const auto prior = hazard_.summary_at(node, stamp);
      double lambda = job.lambda_i / std::max<size_t>(1, hazard_nodes.size());
      if (prior && (prior->confidence_available || prior->last_sample_ns > 0 ||
                    prior->event_count > 0 || job.lambda_i == 0))
        lambda =
            hazard_.confidence_upper_bound(node, stamp).value_or(prior->lambda);
      input.lambda_i += lambda;
    }
    P2GangQuery selection;
    selection.source_nodes = sources;
    selection.rank_query.job_id = job.job_id;
    selection.rank_query.checkpoint_size_b = rank_bytes(job);
    selection.rank_query.vram_required_bytes = rank_vram(job);
    selection.rank_query.gpu_count_required = job.gpu_count_required;
    selection.rank_query.cuda_capability_required =
        job.cuda_capability_required;
    selection.rank_query.remaining_runtime_s = job.remaining_runtime_s;
    selection.rank_query.require_deadline = false;
    selection.rank_query.decision_ns = stamp;
    selection.rank_query.k_min = cfg_.p2_k_min;
    selection.rank_query.load_limit_pct = cfg_.p2_load_limit_pct;
    for (uint32_t rank = 0; rank < job.rank_count; ++rank) {
      auto query = selection.rank_query;
      query.checkpoint_size_b = rank_bytes(job, rank);
      query.vram_required_bytes = rank_vram(job, rank);
      selection.rank_queries.push_back(std::move(query));
    }
    auto replicas = p2_->select_gang(selection);
    if (replicas.success)
      targets[job.job_id] = replicas.selected;
    std::map<std::string, long double> counts;
    if (replicas.success)
      for (size_t rank = 0; rank < sources.size(); ++rank) {
        auto path =
            topo_->shortest_path(sources[rank], replicas.selected[rank]);
        if (!path)
          continue;
        for (size_t i = 1; i < path->path.size(); ++i) {
          auto src = path->path[i - 1], dst = path->path[i];
          if (dst < src)
            std::swap(src, dst);
          const auto key = std::to_string(src.size()) + ":" + src + dst;
          counts[key] += rank_share(job, static_cast<uint32_t>(rank));
          for (const auto &link : topo_->links_from(src))
            if (link.dst == dst)
              edge_capacity[key] = capacities_.lower_bound_bps(
                  src, dst, stamp, link.bandwidth_bps);
        }
      }
    for (const auto &[edge, count] : counts) {
      double cap = edge_capacity[edge];
      if (input.write_cap_bps <= 0 || cap < input.write_cap_bps)
        input.write_cap_bps = cap;
    }
    weight_sum += std::cbrt(static_cast<long double>(input.lambda_i) *
                            input.checkpoint_size_b);
    inputs.push_back(input);
    edge_counts.push_back(std::move(counts));
  }
  double budget = cfg_.B_ckpt_bps;
  if (weight_sum > 0)
    for (const auto &[edge, capacity] : edge_capacity) {
      long double demand = 0;
      for (size_t i = 0; i < inputs.size(); ++i) {
        const auto found = edge_counts[i].find(edge);
        if (found != edge_counts[i].end())
          demand += found->second *
                    std::cbrt(static_cast<long double>(inputs[i].lambda_i) *
                              inputs[i].checkpoint_size_b);
      }
      if (demand > 0)
        budget = std::min(budget,
                          static_cast<double>(capacity * weight_sum / demand));
    }
  p1_->set_budget_bps(budget);
  auto result = p1_->compute(inputs);
  const auto shared_paths_feasible = [&](const P1EpochResult &allocation) {
    for (const auto &edge : edge_capacity) {
      long double used = 0;
      for (size_t i = 0; i < allocation.schedules.size(); ++i) {
        auto count = edge_counts[i].find(edge.first);
        if (count != edge_counts[i].end())
          used += static_cast<long double>(allocation.schedules[i].b_i_bps) *
                  count->second;
      }
      if (used > edge.second * (1 + 1e-12L))
        return false;
    }
    return true;
  };
  if (!shared_paths_feasible(result)) {
    double low = 0, high = budget;
    for (unsigned iteration = 0; iteration < 60; ++iteration) {
      const double middle = low + (high - low) / 2;
      p1_->set_budget_bps(middle);
      auto candidate = p1_->compute(inputs);
      if (shared_paths_feasible(candidate))
        low = middle;
      else
        high = middle;
    }
    budget = low;
    p1_->set_budget_bps(budget);
    result = p1_->compute(inputs);
  }
  for (size_t i = 0; i < result.schedules.size(); ++i) {
    const auto &schedule = result.schedules[i];
    const auto &job = jobs[i];
    job_reg_.update(job.job_id, [&](JobRecord &record) {
      record.checkpoint_interval_s = schedule.delta_t_s;
      record.checkpoint_allocated_bps = schedule.b_i_bps;
      record.checkpoint_write_rate_bps = schedule.write_rate_limit_bps;
      record.checkpoint_constraints_feasible = schedule.feasible;
    });
    persist_job_locked(job.job_id);
    const auto previous = checkpoint_plans_.find(job.job_id);
    std::vector<CheckpointCommand> plans;
    for (uint32_t rank = 0; rank < job.rank_count; ++rank) {
      CheckpointCommand command;
      command.job_id = job.job_id;
      command.interval_s = schedule.delta_t_s;
      command.allocated_bps = schedule.b_i_bps * rank_share(job, rank);
      command.write_rate_limit_bps =
          schedule.write_rate_limit_bps * rank_share(job, rank);
      command.feasible = schedule.feasible && targets.count(job.job_id);
      command.rank = rank;
      command.gang_size = job.rank_count;
      if (targets.count(job.job_id)) {
        command.replica_node_id = targets[job.job_id][rank];
        if (auto node = node_reg_.get(command.replica_node_id))
          command.replica_address = node->address;
        command.replica_port =
            migration_ports_.count(command.replica_node_id)
                ? migration_ports_.at(command.replica_node_id)
                : 7600;
      }
      if (previous != checkpoint_plans_.end() &&
          rank < previous->second.size()) {
        command.eligible_start_ns = previous->second[rank].eligible_start_ns;
        command.checkpoint_id = previous->second[rank].checkpoint_id;
        command.migration_id = previous->second[rank].migration_id;
        command.checkpoint_sequence =
            previous->second[rank].checkpoint_sequence;
        if (!command.migration_id.empty() &&
            p3_->get_reservation(job.job_id, rank, command.migration_id)) {
          command.replica_node_id = previous->second[rank].replica_node_id;
          command.replica_address = previous->second[rank].replica_address;
          command.replica_port = previous->second[rank].replica_port;
        }
      } else {
        const long double offset = schedule.feasible
                                       ? schedule.delta_t_s * 1e9L * i /
                                             std::max<size_t>(1, jobs.size())
                                       : 0;
        command.eligible_start_ns =
            offset < std::numeric_limits<int64_t>::max() - stamp
                ? stamp + static_cast<int64_t>(offset)
                : stamp;
      }
      plans.push_back(std::move(command));
    }
    checkpoint_plans_[job.job_id] = std::move(plans);
    replica_nodes_[job.job_id] = targets[job.job_id];
  }
#if RECLAIMNET_HAVE_JSON
  if (writer_) {
    J members = J::array();
    for (size_t i = 0; i < result.schedules.size(); ++i) {
      const auto &schedule = result.schedules[i];
      const auto &input = inputs[i];
      members.push_back(
          {{"job_id", input.job_id},
           {"source_node_ids", sources_of(jobs[i])},
           {"replica_node_ids", targets[input.job_id]},
           {"checkpoint_size_bytes", input.checkpoint_size_b},
           {"lambda_e", input.lambda_i},
           {"allocation_weight",
            std::cbrt(input.lambda_i * input.checkpoint_size_b)},
           {"write_cap_bps", input.write_cap_bps},
           {"b_i_bps", schedule.b_i_bps},
           {"write_rate_limit_bps", schedule.write_rate_limit_bps},
           {"p1_raw_interval_s", std::isfinite(schedule.raw_delta_t_s)
                                     ? schedule.raw_delta_t_s
                                     : std::numeric_limits<double>::max()},
           {"p1_lower_interval_s", std::isfinite(schedule.lower_bound_s)
                                       ? schedule.lower_bound_s
                                       : std::numeric_limits<double>::max()},
           {"p1_raw_interval_unbounded",
            !std::isfinite(schedule.raw_delta_t_s)},
           {"p1_upper_interval_s", schedule.upper_bound_s},
           {"target_interval_s", schedule.delta_t_s},
           {"constraints_feasible", schedule.feasible},
           {"hazard_upper_confidence_bound_available",
            hazard_.confidence_upper_bound(jobs[i].node_id, stamp)
                .has_value()}});
    }
    trace_locked(
        "p1_allocation_epochs.jsonl",
        J{{"run_id", cfg_.run_id},
          {"allocation_epoch_id", "p1-" + std::to_string(stamp)},
          {"start_ns", stamp},
          {"end_ns", stamp + static_cast<int64_t>(cfg_.epoch_seconds * 1e9)},
          {"control_epoch_s", cfg_.epoch_seconds},
          {"allocation_budget_bps", budget},
          {"controller_ceiling_bps", cfg_.B_ckpt_bps},
          {"beta", cfg_.checkpoint_beta},
          {"total_allocated_bps", result.total_bps_used},
          {"water_fill_iterations", result.water_fill_iters},
          {"members", members}}
            .dump());
  }
#endif
}
void Coordinator::apply_batch_locked(const BatchAdmissionResult &batch,
                                     int64_t stamp) {
  for (const auto &update : batch.rate_updates) {
    auto &pending = pending_rates_[update.src];
    auto found =
        std::find_if(pending.begin(), pending.end(), [&](const auto &value) {
          return value.migration_id == update.migration_id &&
                 value.rank == update.rank && value.job_id == update.job_id;
        });
    if (found == pending.end())
      pending.push_back(update);
    else
      *found = update;
    if (pending.size() > 512)
      pending.erase(pending.begin(),
                    pending.begin() + static_cast<long>(pending.size() - 512));
  }
  std::set<JobId> failed;
  for (const auto &degraded : batch.degraded) {
    if (!degraded.recovery_required ||
        !failed.insert(degraded.entry.job_id).second)
      continue;
    auto job = job_reg_.get(degraded.entry.job_id);
    if (job)
      recovery_locked(*job, sources_of(*job), stamp);
  }
}
void Coordinator::run_one_epoch(int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return;
  if (stamp <= 0)
    stamp = now_ns();
  for (auto pending = pending_invalidations_.begin();
       pending != pending_invalidations_.end();) {
    if (pending->second > stamp) {
      ++pending;
      continue;
    }
    const auto id = pending->first;
    const int64_t departure = pending->second;
    catalog_.invalidate_node(id, departure);
    store_->set_runtime("invalidation", id, std::to_string(departure));
    if (auto node = node_reg_.get(id)) {
      node->state = NodeState::OFFLINE;
      node_reg_.upsert(*node);
      store_->upsert_node(*node);
    }
#if RECLAIMNET_HAVE_JSON
    trace_locked("node_storage_invalidations.jsonl",
                 J{{"node_id", id},
                   {"timestamp_ns", departure},
                   {"reason", "SCHEDULED_RECLAIM"}}
                     .dump());
#endif
    for (const auto &job : job_reg_.by_state(JobState::RUNNING)) {
      const auto sources = sources_of(job);
      if (std::find(sources.begin(), sources.end(), id) != sources.end())
        recovery_locked(job, {id}, departure);
    }
    pending = pending_invalidations_.erase(pending);
    store_->erase_runtime("pending_reclaim_deadline", id);
  }
  auto stale = node_reg_.mark_stale(
      stamp - static_cast<int64_t>(cfg_.heartbeat_timeout_s * 1e9));
  for (const auto &node : stale) {
    catalog_.invalidate_node(node, stamp);
    store_->set_runtime("invalidation", node, std::to_string(stamp));
    if (auto record = node_reg_.get(node))
      store_->upsert_node(*record);
    for (const auto &job : job_reg_.all()) {
      const auto sources = sources_of(job);
      if (job.state == JobState::RUNNING &&
          std::find(sources.begin(), sources.end(), node) != sources.end())
        recovery_locked(job, {node}, stamp);
    }
  }
  for (const auto &node : topo_->all_nodes())
    for (const auto &link : topo_->links_from(node))
      p3_->add_link(link.src, link.dst,
                    capacities_.lower_bound_bps(link.src, link.dst, stamp,
                                                link.bandwidth_bps));
  apply_batch_locked(p3_->admit_batch({}, stamp), stamp);
  allocate_checkpoints_locked(stamp);
  for (auto &item : recovery_barriers_)
    for (auto source = item.second.waiting.begin();
         source != item.second.waiting.end();) {
      auto record = node_reg_.get(*source);
      if (!record || record->state == NodeState::OFFLINE) {
        auto &queued = stop_queue_[*source];
        queued.erase(std::remove(queued.begin(), queued.end(),
                                 std::make_pair(item.first, item.second.id)),
                     queued.end());
        source = item.second.waiting.erase(source);
      } else
        ++source;
    }
  for (const auto &job : job_reg_.by_state(JobState::SUSPENDED))
    if (!recovery_queue_.count(job.job_id))
      recovery_locked(job, sources_of(job), stamp);
}
std::vector<CheckpointCommand>
Coordinator::checkpoint_commands(const NodeId &node) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  std::vector<CheckpointCommand> result;
  for (const auto &job : job_reg_.by_state(JobState::RUNNING)) {
    const auto sources = sources_of(job);
    auto plans = checkpoint_plans_.find(job.job_id);
    if (plans == checkpoint_plans_.end())
      continue;
    for (size_t rank = 0; rank < sources.size() && rank < plans->second.size();
         ++rank)
      if (sources[rank] == node)
        result.push_back(plans->second[rank]);
  }
  return result;
}
std::vector<MigrationEntry>
Coordinator::rate_updates(const NodeId &node) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  auto found = pending_rates_.find(node);
  return found == pending_rates_.end() ? std::vector<MigrationEntry>{}
                                       : found->second;
}
std::vector<ReplicaCheckpoint>
Coordinator::recovery_commands(const NodeId &node) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  std::vector<ReplicaCheckpoint> result;
  for (const auto &[id, checkpoints] : recovery_queue_) {
    auto barrier = recovery_barriers_.find(id);
    if (barrier != recovery_barriers_.end() && !barrier->second.waiting.empty())
      continue;
    for (const auto &checkpoint : checkpoints)
      if (checkpoint.replica_node_id == node)
        result.push_back(checkpoint);
  }
  return result;
}
std::vector<std::pair<JobId, std::string>>
Coordinator::start_jobs(const NodeId &node) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  auto found = resume_queue_.find(node);
  return found == resume_queue_.end()
             ? std::vector<std::pair<JobId, std::string>>{}
             : found->second;
}
VoidResult
Coordinator::queue_resume_locked(const JobId &id, const std::string &barrier,
                                 const std::vector<NodeId> &destinations) {
  for (auto &item : resume_queue_)
    item.second.erase(std::remove_if(item.second.begin(), item.second.end(),
                                     [&](const auto &command) {
                                       return command.first == id;
                                     }),
                      item.second.end());
  for (const auto &node : destinations)
    resume_queue_[node].emplace_back(id, barrier);
#if RECLAIMNET_HAVE_JSON
  return store_->set_runtime(
      "resume_barrier", id,
      J{{"barrier_id", barrier}, {"destinations", destinations}}.dump());
#else
  return ok_void();
#endif
}
std::vector<std::pair<JobId, std::string>>
Coordinator::stop_jobs(const NodeId &node) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  auto found = stop_queue_.find(node);
  return found == stop_queue_.end()
             ? std::vector<std::pair<JobId, std::string>>{}
             : found->second;
}
VoidResult Coordinator::source_stopped(const NodeId &node, const JobId &id,
                                       const std::string &barrier_id,
                                       int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  if (stopped_sources_[barrier_id].count(node))
    return ok_void();
  auto barrier = recovery_barriers_.find(id);
  if (barrier == recovery_barriers_.end() || barrier->second.id != barrier_id ||
      !barrier->second.waiting.count(node))
    return err_void(
        "Source stop acknowledgement does not match its recovery barrier");
  barrier->second.waiting.erase(node);
  stopped_sources_[barrier_id].insert(node);
  auto &queued = stop_queue_[node];
  queued.erase(
      std::remove(queued.begin(), queued.end(), std::make_pair(id, barrier_id)),
      queued.end());
#if RECLAIMNET_HAVE_JSON
  trace_locked("events.jsonl", J{{"type", "RECOVERY_SOURCE_STOPPED"},
                                 {"node_id", node},
                                 {"job_id", id},
                                 {"barrier_id", barrier_id},
                                 {"timestamp_ns", stamp > 0 ? stamp : now_ns()}}
                                   .dump());
#else
  (void)stamp;
#endif
  return ok_void();
}
VoidResult Coordinator::export_flow(const std::string &record) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
#if RECLAIMNET_HAVE_JSON
  try {
    auto value = J::parse(record);
    auto path =
        topo_->shortest_path(value.at("src_node_id"), value.at("dst_node_id"));
    if (!path)
      return err_void("Reported flow has no known topology path");
    J edges = J::array();
    for (size_t i = 1; i < path->path.size(); ++i)
      edges.push_back({path->path[i - 1], path->path[i]});
    value["path_edges"] = edges;
    const char *names[] = {"IntraRack", "IntraBuilding", "CrossBuilding",
                           "CoreCrossing"};
    value["topology_level"] = names[static_cast<unsigned>(path->max_level)];
    value["path_source"] = "coordinator_topology";
    value["run_id"] = cfg_.run_id;
    if (auto parsed = trace::Parse<trace::Flow>(value.dump()); !parsed)
      return parsed.error();
    trace_locked("flows.jsonl", value.dump());
    return ok_void();
  } catch (const std::exception &error) {
    return Error{error.what()};
  }
#else
  (void)record;
  return writer_ ? err_void("Flow export requires JSON") : ok_void();
#endif
}
std::string Coordinator::runtime_spec(const JobId &id) const {
  std::lock_guard<std::mutex> lock(control_mu_);
  auto found = runtime_specs_.find(id);
  return found == runtime_specs_.end() ? std::string{} : found->second;
}
Result<MigrationCommand> Coordinator::recovery_locked(
    const JobRecord &job, const std::vector<NodeId> &excluded, int64_t stamp) {
  if (!recovery_barriers_.count(job.job_id)) {
    if (auto erased = store_->erase_runtime("resume_barrier", job.job_id);
        !erased)
      return erased.error();
    RecoveryBarrier barrier;
    barrier.id = "recovery-stop:" + job.job_id + ":" + std::to_string(stamp);
    for (const auto &source : sources_of(job)) {
      auto record = node_reg_.get(source);
      if (record && (record->state == NodeState::ONLINE ||
                     record->state == NodeState::DEGRADED ||
                     record->state == NodeState::DRAINING)) {
        barrier.waiting.insert(source);
        stop_queue_[source].emplace_back(job.job_id, barrier.id);
      }
    }
    recovery_barriers_[job.job_id] = std::move(barrier);
    for (auto &queued : resume_queue_)
      queued.second.erase(std::remove_if(queued.second.begin(),
                                         queued.second.end(),
                                         [&](const auto &command) {
                                           return command.first == job.job_id;
                                         }),
                          queued.second.end());
  }
  auto &barrier = recovery_barriers_.at(job.job_id);
  for (auto source = barrier.waiting.begin();
       source != barrier.waiting.end();) {
    auto record = node_reg_.get(*source);
    if (!record || record->state == NodeState::OFFLINE) {
      auto &queued = stop_queue_[*source];
      queued.erase(std::remove(queued.begin(), queued.end(),
                               std::make_pair(job.job_id, barrier.id)),
                   queued.end());
      source = barrier.waiting.erase(source);
    } else
      ++source;
  }
  std::vector<NodeId> available;
  for (const auto &node : node_reg_.by_state(NodeState::ONLINE)) {
    if (node.gpu_count >= job.gpu_count_required &&
        node.cuda_capability >= job.cuda_capability_required)
      available.push_back(node.node_id);
  }
  const auto copies = catalog_.all();
  const auto eligible = [&](const ReplicaCheckpoint &checkpoint) {
    auto node = node_reg_.get(checkpoint.replica_node_id);
    if (!node || checkpoint.rank >= job.rank_count)
      return false;
    auto session = migration_sessions_.find(job.job_id);
    if (session != migration_sessions_.end())
      for (const auto &copy : copies)
        if (copy.job_id == job.job_id &&
            copy.checkpoint_id == session->second &&
            copy.replica_node_id == node->node_id &&
            copy.rank == checkpoint.rank && copy.completed_at_ns <= stamp &&
            restored_ranks_[session->second].count(copy.rank))
          return true;
    return node->gpu_free_count >= job.gpu_count_required &&
           node->telemetry.vram_free_bytes >= rank_vram(job, checkpoint.rank);
  };
  auto replicas = catalog_.freshest_surviving(job.job_id, available, excluded,
                                              stamp, eligible);
  for (const auto &entry : p3_->active_migrations())
    if (entry.job_id == job.job_id) {
      auto stopped = entry;
      stopped.allocated_bps = 0;
      ++stopped.allocation_version;
      pending_rates_[entry.src].push_back(stopped);
    }
  job_reg_.update(job.job_id, [](JobRecord &record) {
    record.state = JobState::SUSPENDED;
    record.migration_dst.clear();
  });
  persist_job_locked(job.job_id);
  p3_->release(job.job_id);
  if (replicas.empty())
    return Error{"No complete surviving checkpoint; recovery remains queued"};
  recovery_queue_[job.job_id] = replicas;
  migration_sessions_[job.job_id] = replicas.front().checkpoint_id;
  auto &destinations = migration_destinations_[job.job_id];
  destinations.resize(replicas.front().gang_size);
  for (const auto &replica : replicas)
    destinations[replica.rank] = replica.replica_node_id;
#if RECLAIMNET_HAVE_JSON
  if (auto saved = store_->set_runtime(
          "migration_target", job.job_id,
          J{{"migration_id", migration_sessions_[job.job_id]},
            {"destinations", destinations}}
              .dump());
      !saved)
    return saved.error();
#endif
  MigrationCommand command;
  command.job_id = job.job_id;
  command.recovery = true;
  if (barrier.waiting.empty())
    command.recoveries = replicas;
  command.dst_node = replicas.front().replica_node_id;
  command.rank = replicas.front().rank;
  command.gang_size = replicas.front().gang_size;
  if (auto node = node_reg_.get(command.dst_node))
    command.destination_address = node->address;
  command.destination_port = migration_ports_.count(command.dst_node)
                                 ? migration_ports_.at(command.dst_node)
                                 : 7600;
  command.migration_id = replicas.front().checkpoint_id;
#if RECLAIMNET_HAVE_JSON
  J selected = J::array();
  uint64_t bytes = 0;
  for (const auto &checkpoint : replicas) {
    selected.push_back(checkpoint_json(checkpoint));
    bytes += checkpoint.size_bytes;
  }
  trace_locked("events.jsonl",
               J{{"type", "RECOVERY_START"},
                 {"job_id", job.job_id},
                 {"timestamp_ns", stamp},
                 {"selected_checkpoint_id", command.migration_id},
                 {"recoveries", selected}}
                   .dump());
  trace_locked("migrations.jsonl",
               J{{"run_id", cfg_.run_id},
                 {"migration_id", "recovery:" + command.migration_id},
                 {"job_id", job.job_id},
                 {"src_node_id", job.node_id},
                 {"dst_node_id", command.dst_node},
                 {"reclaim_type", "EMERGENCY"},
                 {"phase", "RESTORING"},
                 {"topology_level", "IntraRack"},
                 {"p3_applicable", false},
                 {"p3_status", "NOT_APPLICABLE"},
                 {"bytes_to_transfer_total", uint64_t{0}},
                 {"checkpoint_size_bytes", bytes},
                 {"deadline_s", 0.0},
                 {"decision_ns", stamp},
                 {"start_ns", stamp},
                 {"end_ns", stamp},
                 {"restore_time_s", cfg_.restore_time_s},
                 {"path_allocations", J::array()},
                 {"selected_checkpoint_id_at_decision", command.migration_id}}
                   .dump());
#endif
  return command;
}
Result<MigrationCommand>
Coordinator::migration_command_locked(const JobRecord &job,
                                      int64_t stamp) const {
  std::vector<MigrationEntry> entries;
  for (const auto &entry : p3_->active_migrations())
    if (entry.job_id == job.job_id &&
        entry.traffic_class != MigrationTrafficClass::PERIODIC_CHECKPOINT)
      entries.push_back(entry);
  if (entries.empty())
    return Error{"No active planned migration"};
  std::sort(entries.begin(), entries.end(),
            [](const auto &a, const auto &b) { return a.rank < b.rank; });
  const auto &first = entries.front();
  if (first.deadline_ns <= stamp)
    return Error{"Migration deadline has expired"};
  MigrationCommand command;
  command.job_id = job.job_id;
  command.dst_node = first.dst;
  command.allocated_bps = first.allocated_bps;
  command.deadline_s = static_cast<double>(first.deadline_ns - stamp) / 1e9;
  command.deadline_ns = first.deadline_ns;
  command.migration_id = first.migration_id;
  command.gang_size = first.gang_size;
  command.eligible_start_ns = first.eligible_start_ns;
  if (auto node = node_reg_.get(first.dst))
    command.destination_address = node->address;
  command.destination_port =
      migration_ports_.count(first.dst) ? migration_ports_.at(first.dst) : 7600;
  for (const auto &entry : entries) {
    RankMigrationCommand rank;
    rank.rank = entry.rank;
    rank.source_node = entry.src;
    rank.destination_node = entry.dst;
    rank.allocated_bps = entry.allocated_bps;
    rank.checkpoint_bytes = entry.data_total_bytes;
    rank.eligible_start_ns = entry.eligible_start_ns;
    rank.restore_time_s = entry.restore_time_s;
    rank.allocation_version = entry.allocation_version;
    auto sequence = checkpoint_sequence_ids_.find(entry.migration_id);
    if (sequence != checkpoint_sequence_ids_.end())
      rank.checkpoint_sequence = sequence->second;
    if (auto node = node_reg_.get(entry.dst))
      rank.destination_address = node->address;
    rank.destination_port = migration_ports_.count(entry.dst)
                                ? migration_ports_.at(entry.dst)
                                : 7600;
    command.ranks.push_back(std::move(rank));
  }
  return command;
}
VoidResult Coordinator::on_reclaim_event(const NodeId &node, const JobId &id,
                                         double notice, ReclaimType type,
                                         int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  if (!node_reg_.get(node) || !std::isfinite(notice) || notice < 0 ||
      (type != ReclaimType::SCHEDULED && type != ReclaimType::EMERGENCY) ||
      (notice == 0 && type != ReclaimType::EMERGENCY))
    return err_void("Invalid reclaim event");
  if (!id.empty()) {
    auto job = job_reg_.get(id);
    if (!job)
      return err_void("Unknown reclaim job");
    const auto sources = sources_of(*job);
    if (std::find(sources.begin(), sources.end(), node) == sources.end())
      return err_void("Source does not own reclaim job");
  }
  if (stamp <= 0)
    stamp = now_ns();
  hazard_.record_event({node, stamp, type});
#if RECLAIMNET_HAVE_JSON
  if (type == ReclaimType::EMERGENCY)
    if (auto saved = store_->set_runtime(
            "hazard_event", node + ":" + std::to_string(stamp),
            J{{"node_id", node}, {"timestamp", stamp}}.dump());
        !saved)
      return saved;
  const auto event = J{
      {"node_id", node},
      {"job_id", id},
      {"tau_seconds", notice},
      {"type", type == ReclaimType::EMERGENCY ? "EMERGENCY" : "SCHEDULED"},
      {"timestamp",
       stamp}}.dump();
  trace_locked("reclaims.jsonl", event);
  return store_->log_event("RECLAIM_SIGNAL", event, stamp);
#else
  return store_->log_event("RECLAIM_SIGNAL", "{}", stamp);
#endif
}
Result<MigrationCommand>
Coordinator::on_reclaim_signal(const NodeId &node, const JobId &job,
                               double notice, ReclaimType type, int64_t stamp) {
  auto result =
      on_reclaim_batch({ReclaimRequest{node, job, notice, type, stamp}});
  return std::move(result.front());
}
std::vector<Result<MigrationCommand>>
Coordinator::on_reclaim_batch(const std::vector<ReclaimRequest> &requests) {
  std::lock_guard<std::mutex> lock(control_mu_);
  std::vector<Result<MigrationCommand>> results(
      requests.size(), Error{"Migration not admitted"});
  if (!running_)
    return results;
  const int64_t current = now_ns();
  std::vector<MigrationEntry> entries;
  std::vector<std::pair<size_t, JobId>> pending;
  std::unordered_map<JobId, size_t> pending_jobs;
  std::vector<std::pair<size_t, size_t>> aliases;
  auto resources = destination_capacity_locked();
  for (const auto &active : p3_->active_migrations())
    if (active.traffic_class == MigrationTrafficClass::PLANNED_MIGRATION) {
      auto &available = resources[active.dst];
      available.first -= std::min(available.first, active.vram_required_bytes);
      available.second -= std::min(available.second, active.gpu_count_required);
    }
  std::vector<size_t> order(requests.size());
  for (size_t i = 0; i < order.size(); ++i)
    order[i] = i;
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    if (requests[a].type != requests[b].type)
      return requests[a].type == ReclaimType::EMERGENCY;
    const auto deadline = [&](size_t i) {
      return std::isfinite(requests[i].notice_s)
                 ? static_cast<long double>(requests[i].timestamp_ns > 0
                                                ? requests[i].timestamp_ns
                                                : current) +
                       requests[i].notice_s * 1e9L
                 : std::numeric_limits<long double>::infinity();
    };
    return deadline(a) < deadline(b);
  });
  int64_t batch_ns = 0;
  for (const size_t index : order) {
    const auto &request = requests[index];
    const int64_t stamp =
        request.timestamp_ns > 0 ? request.timestamp_ns : current;
    if (batch_ns == 0)
      batch_ns = stamp;
    else
      batch_ns = std::max(batch_ns, stamp);
    auto job = job_reg_.get(request.job_id);
    if (!job) {
      results[index] = Error{"Unknown reclaim job"};
      continue;
    }
    const auto sources = sources_of(*job);
    if (std::find(sources.begin(), sources.end(), request.source_node) ==
            sources.end() ||
        !std::isfinite(request.notice_s) || request.notice_s < 0 ||
        (request.type == ReclaimType::SCHEDULED && request.notice_s == 0) ||
        (request.type != ReclaimType::SCHEDULED &&
         request.type != ReclaimType::EMERGENCY)) {
      results[index] = Error{"Invalid reclaim source, type or notice"};
      continue;
    }
    const long double deadline =
        static_cast<long double>(stamp) + request.notice_s * 1e9L;
    if (deadline >
        static_cast<long double>(std::numeric_limits<int64_t>::max())) {
      results[index] = Error{"Reclaim deadline overflow"};
      continue;
    }
    hazard_.record_event({request.source_node, stamp, request.type});
#if RECLAIMNET_HAVE_JSON
    if (request.type == ReclaimType::EMERGENCY)
      if (auto saved = store_->set_runtime(
              "hazard_event", request.source_node + ":" + std::to_string(stamp),
              J{{"node_id", request.source_node}, {"timestamp", stamp}}.dump());
          !saved) {
        results[index] = saved.error();
        continue;
      }
    const auto reclaim =
        J{{"node_id", request.source_node},
          {"job_id", job->job_id},
          {"tau_seconds", request.notice_s},
          {"type",
           request.type == ReclaimType::EMERGENCY ? "EMERGENCY" : "SCHEDULED"},
          {"timestamp", stamp}};
    trace_locked("reclaims.jsonl", reclaim.dump());
    store_->log_event("RECLAIM_SIGNAL", reclaim.dump(), stamp);
#endif
    auto source_record = node_reg_.get(request.source_node);
    if (source_record) {
      source_record->state = request.type == ReclaimType::EMERGENCY
                                 ? NodeState::OFFLINE
                                 : NodeState::DRAINING;
      node_reg_.upsert(*source_record);
      store_->upsert_node(*source_record);
    }
    if (request.type == ReclaimType::EMERGENCY) {
      catalog_.invalidate_node(request.source_node, stamp);
      store_->set_runtime("invalidation", request.source_node,
                          std::to_string(stamp));
#if RECLAIMNET_HAVE_JSON
      trace_locked("node_storage_invalidations.jsonl",
                   J{{"node_id", request.source_node},
                     {"timestamp_ns", stamp},
                     {"reason", "EMERGENCY_RECLAIM"}}
                       .dump());
#endif
      entries.erase(std::remove_if(entries.begin(), entries.end(),
                                   [&](const auto &entry) {
                                     return entry.job_id == job->job_id;
                                   }),
                    entries.end());
      if (auto queued = pending_jobs.find(job->job_id);
          queued != pending_jobs.end()) {
        aliases.emplace_back(queued->second, index);
        pending_jobs.erase(queued);
        pending.erase(std::remove_if(pending.begin(), pending.end(),
                                     [&](const auto &item) {
                                       return item.second == job->job_id;
                                     }),
                      pending.end());
      }
      results[index] = recovery_locked(*job, sources, stamp);
      continue;
    }
    const auto existing_deadline =
        pending_invalidations_.find(request.source_node);
    if (existing_deadline == pending_invalidations_.end() ||
        deadline < existing_deadline->second)
      pending_invalidations_[request.source_node] =
          static_cast<int64_t>(deadline);
    if (auto saved = store_->set_runtime(
            "pending_reclaim_deadline", request.source_node,
            std::to_string(pending_invalidations_[request.source_node]));
        !saved) {
      results[index] = saved.error();
      continue;
    }
    if (auto queued = pending_jobs.find(job->job_id);
        queued != pending_jobs.end()) {
      aliases.emplace_back(index, queued->second);
      continue;
    }
    if (job->state == JobState::MIGRATING) {
      results[index] = migration_command_locked(*job, stamp);
      continue;
    }
    if (job->state != JobState::RUNNING) {
      results[index] = Error{"Job is not running"};
      continue;
    }
    P2GangQuery query;
    query.source_nodes = sources;
    auto &q = query.rank_query;
    q.job_id = job->job_id;
    q.checkpoint_size_b = rank_bytes(*job);
    q.vram_required_bytes = rank_vram(*job);
    q.gpu_count_required = job->gpu_count_required;
    q.cuda_capability_required = job->cuda_capability_required;
    q.remaining_runtime_s = job->remaining_runtime_s;
    q.decision_ns = stamp;
    q.reclaim_deadline_s = request.notice_s;
    q.T_restore_s = cfg_.restore_time_s;
    q.migration_share = cfg_.migration_share;
    q.research_min_bps = cfg_.research_min_bps;
    q.k_min = cfg_.p2_k_min;
    q.load_limit_pct = cfg_.p2_load_limit_pct;
    q.destination_resources = resources;
    for (uint32_t rank = 0; rank < job->rank_count; ++rank) {
      auto single = q;
      single.checkpoint_size_b = rank_bytes(*job, rank);
      single.vram_required_bytes = rank_vram(*job, rank);
      query.rank_queries.push_back(std::move(single));
    }
    auto selected = p2_->select_gang(query);
    if (!selected.success) {
      results[index] = recovery_locked(*job, sources, stamp);
      continue;
    }
    const std::string id = "migration:" + job->job_id + ":" +
                           std::to_string(next_migration_id_++) + ":" +
                           std::to_string(stamp);
    checkpoint_sequence_ids_[id] = ++checkpoint_sequences_[job->job_id];
    for (uint32_t rank = 0; rank < job->rank_count; ++rank) {
      auto path = topo_->shortest_path(sources[rank], selected.selected[rank]);
      if (!path)
        continue;
      MigrationEntry entry;
      entry.job_id = job->job_id;
      entry.src = sources[rank];
      entry.dst = selected.selected[rank];
      entry.data_remaining_bytes = rank_bytes(*job, rank);
      entry.start_ns = stamp;
      entry.deadline_s = request.notice_s;
      entry.restore_time_s = cfg_.restore_time_s;
      entry.path_nodes = path->path;
      entry.rank = rank;
      entry.gang_size = job->rank_count;
      entry.migration_id = id;
      entry.emergency = request.type == ReclaimType::EMERGENCY;
      entry.minimum_useful_transfer_s = cfg_.minimum_useful_transfer_s;
      entry.vram_required_bytes = rank_vram(*job, rank);
      entry.gpu_count_required = job->gpu_count_required;
      auto &available = resources[entry.dst];
      available.first -= std::min(available.first, entry.vram_required_bytes);
      available.second -= std::min(available.second, entry.gpu_count_required);
      entries.push_back(std::move(entry));
    }
    pending.emplace_back(index, job->job_id);
    pending_jobs[job->job_id] = index;
#if RECLAIMNET_HAVE_JSON
    const std::string decision = "p2-" + id;
    for (size_t rank = 0; rank < selected.ranks.size(); ++rank)
      for (const auto &candidate : selected.ranks[rank].ranked)
        trace_locked(
            "p2_candidates.jsonl",
            J{{"run_id", cfg_.run_id},
              {"decision_id", decision},
              {"job_id", job->job_id},
              {"decision_ns", stamp},
              {"source_node_ids", sources},
              {"destination_node_ids", {candidate.node_id}},
              {"transfer_bytes", rank_bytes(*job)},
              {"path_bottleneck_bps", candidate.bottleneck_bps},
              {"transfer_time_s", candidate.T_transfer_s},
              {"restore_time_s", cfg_.restore_time_s},
              {"remaining_runtime_s", job->remaining_runtime_s},
              {"survival_probability", candidate.survival_prob},
              {"historical_mean_migration_s",
               candidate.historical_migration_cost_s},
              {"score", std::isfinite(candidate.composite_cost)
                            ? J(candidate.composite_cost)
                            : J(nullptr)},
              {"same_building", candidate.same_building},
              {"feasible", candidate.feasible},
              {"selected", candidate.node_id == selected.selected[rank]},
              {"rank", rank}}
                .dump());
#endif
  }
  if (!entries.empty()) {
    const auto batch = p3_->admit_batch(entries, batch_ns);
    apply_batch_locked(batch, batch_ns);
    for (const auto &item : pending) {
      const size_t index = item.first;
      const JobId id = item.second;
      auto job = job_reg_.get(id);
      std::vector<NodeId> destinations(job->rank_count);
      for (const auto &entry : batch.admitted)
        if (entry.job_id == id &&
            entry.traffic_class != MigrationTrafficClass::PERIODIC_CHECKPOINT &&
            entry.rank < destinations.size())
          destinations[entry.rank] = entry.dst;
      if (std::any_of(destinations.begin(), destinations.end(),
                      [](const auto &node) { return node.empty(); })) {
        results[index] = recovery_locked(*job, sources_of(*job), batch_ns);
        continue;
      }
      migration_destinations_[id] = destinations;
      for (const auto &admitted : batch.admitted)
        if (admitted.job_id == id &&
            admitted.traffic_class ==
                MigrationTrafficClass::PLANNED_MIGRATION) {
          migration_sessions_[id] = admitted.migration_id;
          break;
        }
#if RECLAIMNET_HAVE_JSON
      if (auto saved =
              store_->set_runtime("migration_target", id,
                                  J{{"migration_id", migration_sessions_[id]},
                                    {"destinations", destinations}}
                                      .dump());
          !saved) {
        results[index] = saved.error();
        continue;
      }
#endif
      store_->erase_runtime("resume_barrier", id);
      for (auto &queued : resume_queue_)
        queued.second.erase(std::remove_if(queued.second.begin(),
                                           queued.second.end(),
                                           [&](const auto &command) {
                                             return command.first == id;
                                           }),
                            queued.second.end());
      job_reg_.update(id, [&](JobRecord &record) {
        record.state = JobState::MIGRATING;
        record.migration_dst = destinations.front();
      });
      persist_job_locked(id);
      results[index] =
          migration_command_locked(job_reg_.get(id).value(), batch_ns);
#if RECLAIMNET_HAVE_JSON
      if (results[index]) {
        const auto &command = results[index].value();
        J paths = J::array();
        for (const auto &entry : batch.admitted)
          if (entry.job_id == id &&
              entry.migration_id == command.migration_id) {
            J edges = J::array();
            for (size_t i = 1; i < entry.path_nodes.size(); ++i)
              edges.push_back({entry.path_nodes[i - 1], entry.path_nodes[i]});
            paths.push_back({{"rank", entry.rank},
                             {"src_node_id", entry.src},
                             {"dst_node_id", entry.dst},
                             {"bytes", entry.data_total_bytes},
                             {"required_bps", entry.minimum_required_bps},
                             {"allocated_bps", entry.allocated_bps},
                             {"path_edges", edges}});
          }
        trace_locked(
            "migrations.jsonl",
            J{{"run_id", cfg_.run_id},
              {"migration_id", command.migration_id},
              {"job_id", id},
              {"src_node_id", requests[index].source_node},
              {"dst_node_id", command.dst_node},
              {"reclaim_type", requests[index].type == ReclaimType::EMERGENCY
                                   ? "EMERGENCY"
                                   : "SCHEDULED"},
              {"phase", "ADMITTED"},
              {"topology_level",
               [&]() {
                 auto level = TopologyLevel::INTRA_RACK;
                 for (const auto &entry : batch.admitted)
                   if (entry.job_id == id)
                     if (auto path = topo_->shortest_path(entry.src, entry.dst))
                       level = std::max(level, path->max_level);
                 const char *names[] = {"IntraRack", "IntraBuilding",
                                        "CrossBuilding", "CoreCrossing"};
                 return std::string(names[static_cast<unsigned>(level)]);
               }()},
              {"p3_applicable", true},
              {"p3_status", "ADMITTED"},
              {"bytes_to_transfer_total", job->checkpoint_size},
              {"deadline_s", requests[index].notice_s},
              {"deadline_ns", command.deadline_ns},
              {"decision_ns", batch_ns},
              {"start_ns", batch_ns},
              {"end_ns", batch_ns},
              {"restore_time_s", cfg_.restore_time_s},
              {"available_transfer_s",
               requests[index].notice_s - cfg_.restore_time_s},
              {"path_allocations", paths},
              {"allocation_method", "EDF_THEN_MAX_MIN_WATER_FILL"}}
                .dump());
      }
#endif
    }
  }
  for (size_t round = 0; round < aliases.size(); ++round)
    for (const auto &alias : aliases)
      results[alias.first] = results[alias.second];
  return results;
}
Result<CheckpointCommand>
Coordinator::begin_checkpoint(const NodeId &source, const JobId &id,
                              uint64_t size, int64_t stamp, uint32_t rank) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return Error{"Coordinator is not running"};
  if (stamp <= 0)
    stamp = now_ns();
  auto job = job_reg_.get(id);
  if (!job || job->state != JobState::RUNNING || rank >= job->rank_count ||
      sources_of(*job)[rank] != source || size == 0)
    return Error{"Invalid checkpoint source, rank or payload"};
  if (!checkpoint_plans_.count(id))
    allocate_checkpoints_locked(stamp);
  auto plans = checkpoint_plans_.find(id);
  if (plans == checkpoint_plans_.end() || rank >= plans->second.size() ||
      !plans->second[rank].feasible)
    return Error{"Checkpoint constraints or replica placement are infeasible"};
  std::string migration = plans->second[rank].migration_id;
  if (migration.empty() ||
      committed_ranks_[migration].size() >= job->rank_count) {
    migration = "checkpoint:" + id + ":" +
                std::to_string(++checkpoint_sequences_[id]) + ":" +
                std::to_string(stamp);
    checkpoint_sequence_ids_[migration] = checkpoint_sequences_[id];
    checkpoint_sizes_[migration].resize(job->rank_count);
    for (uint32_t r = 0; r < job->rank_count; ++r)
      checkpoint_sizes_[migration][r] = rank_bytes(*job, r);
    for (auto &plan : plans->second) {
      plan.migration_id = migration;
      plan.checkpoint_id = migration;
      plan.checkpoint_sequence = checkpoint_sequences_[id];
    }
  }
  if (committed_ranks_[migration].count(rank))
    return Error{"Checkpoint rank already committed"};
  checkpoint_sizes_[migration][rank] = size;
  long double logical_bytes = 0;
  for (uint64_t bytes : checkpoint_sizes_[migration])
    logical_bytes += bytes;
  const double interval =
      std::max(job->checkpoint_interval_s,
               static_cast<double>(8.0L * logical_bytes /
                                   job->checkpoint_write_rate_bps));
  std::vector<PeriodicCheckpointDemand> demands;
  const auto sources = sources_of(*job);
  for (uint32_t r = 0; r < job->rank_count; ++r) {
    PeriodicCheckpointDemand demand;
    demand.job_id = id;
    demand.src = sources[r];
    demand.dst = plans->second[r].replica_node_id;
    demand.checkpoint_size_bytes = checkpoint_sizes_[migration][r];
    demand.interval_s = interval;
    demand.write_rate_limit_bps = static_cast<double>(
        job->checkpoint_write_rate_bps *
        static_cast<long double>(demand.checkpoint_size_bytes) / logical_bytes);
    demand.loss_budget_s = job->loss_budget_s > 0
                               ? job->loss_budget_s
                               : cfg_.checkpoint_loss_budget_s;
    demand.demand_id = migration;
    demand.rank = r;
    demand.gang_size = job->rank_count;
    demand.bytes_done =
        committed_ranks_[migration].count(r) ? demand.checkpoint_size_bytes : 0;
    if (auto path = topo_->shortest_path(demand.src, demand.dst))
      demand.path_nodes = path->path;
    demands.push_back(std::move(demand));
  }
  p3_->release_migration(migration);
  auto batch = p3_->schedule_checkpoints(demands, stamp);
  apply_batch_locked(batch, stamp);
  auto reserved = p3_->get_reservation(id, rank, migration);
  if (!reserved)
    return Error{"P3 deferred the periodic checkpoint"};
  auto command = plans->second[rank];
  command.allocated_bps = reserved->allocated_bps;
  command.interval_s = interval;
  command.deadline_ns = reserved->deadline_ns;
  command.allocation_version = reserved->allocation_version;
  command.write_rate_limit_bps = reserved->allocated_bps;
  command.eligible_start_ns = reserved->eligible_start_ns;
  return command;
}
Result<MigrationEntry>
Coordinator::prepare_final_payload(const JobId &id,
                                   const std::string &migration, uint32_t rank,
                                   uint64_t size, int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return Error{"Coordinator is not running"};
  auto job = job_reg_.get(id);
  auto active = p3_->get_reservation(id, rank, migration);
  if (!job || job->state != JobState::MIGRATING || !active ||
      active->traffic_class != MigrationTrafficClass::PLANNED_MIGRATION ||
      size == 0)
    return Error{"No matching final checkpoint migration"};
  if (active->bytes_done > 0 && active->data_total_bytes != size)
    return Error{"An active transfer payload cannot be replaced"};
  if (stamp <= 0)
    stamp = now_ns();
  if (auto updated = p3_->update_payload(migration, rank, size); !updated)
    return updated.error();
  apply_batch_locked(p3_->admit_batch({}, stamp), stamp);
  auto accepted = p3_->get_reservation(id, rank, migration);
  if (!accepted)
    return Error{"Actual checkpoint payload cannot complete within its reclaim "
                 "deadline"};
  return accepted.value();
}
VoidResult Coordinator::commit_checkpoint(const ReplicaCheckpoint &request,
                                          const std::string &id,
                                          bool restored) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  auto job = job_reg_.get(request.job_id);
  if (!job || request.rank >= job->rank_count ||
      request.gang_size != job->rank_count)
    return err_void("Checkpoint rank does not match the registered job");
  auto checkpoint = request;
  const std::string migration = id.empty() ? checkpoint.checkpoint_id : id;
  if (checkpoint.checkpoint_id != migration)
    return err_void("Checkpoint identity does not match its migration session");
  if (auto active =
          p3_->get_reservation(checkpoint.job_id, checkpoint.rank, migration);
      active && active->dst != checkpoint.replica_node_id)
    return err_void(
        "Checkpoint replica does not match the admitted migration path");
  if (checkpoint_sequence_ids_.count(migration))
    checkpoint.sequence = checkpoint_sequence_ids_[migration];
  if (auto committed = catalog_.commit(checkpoint); !committed)
    return committed;
#if RECLAIMNET_HAVE_JSON
  auto value = checkpoint_json(checkpoint);
  if (auto saved = store_->set_runtime("checkpoint",
                                       checkpoint.checkpoint_id + ":" +
                                           std::to_string(checkpoint.rank),
                                       value.dump());
      !saved)
    return saved;
  value["size_bytes"] = checkpoint.size_bytes;
  value["completed_at"] = checkpoint.completed_at_ns;
  value["node_id"] = checkpoint.replica_node_id;
  value["run_id"] = cfg_.run_id;
  value["restored"] = restored;
  trace_locked("checkpoints.jsonl", value.dump());
#endif
  checkpoint_sequences_[checkpoint.job_id] =
      std::max(checkpoint_sequences_[checkpoint.job_id], checkpoint.sequence);
  committed_ranks_[migration].insert(checkpoint.rank);
  auto &sizes = checkpoint_sizes_[migration];
  if (sizes.size() != checkpoint.gang_size)
    sizes.resize(checkpoint.gang_size);
  sizes[checkpoint.rank] = checkpoint.size_bytes;
  if (committed_ranks_[migration].size() == checkpoint.gang_size) {
    uint64_t logical = 0;
    for (uint64_t size : sizes) {
      if (size > std::numeric_limits<uint64_t>::max() - logical)
        return err_void("Logical checkpoint payload overflow");
      logical += size;
    }
    job_reg_.update(checkpoint.job_id, [&](JobRecord &record) {
      record.checkpoint_size = logical;
      record.rank_checkpoint_sizes = sizes;
    });
    persist_job_locked(checkpoint.job_id);
  }
  if (restored) {
    restored_ranks_[migration].insert(checkpoint.rank);
#if RECLAIMNET_HAVE_JSON
    if (auto saved = store_->set_runtime(
            "restored_rank", migration + ":" + std::to_string(checkpoint.rank),
            J{{"job_id", checkpoint.job_id},
              {"migration_id", migration},
              {"rank", checkpoint.rank}}
                .dump());
        !saved)
      return saved;
#endif
  }
  auto active =
      p3_->get_reservation(checkpoint.job_id, checkpoint.rank, migration);
  if (active) {
    if (auto updated = p3_->update_payload(migration, checkpoint.rank,
                                           checkpoint.size_bytes);
        !updated)
      return updated;
    p3_->update_rank_progress(migration, checkpoint.rank,
                              checkpoint.size_bytes);
  }
  if (active &&
      active->traffic_class == MigrationTrafficClass::PERIODIC_CHECKPOINT) {
    p3_->update_rank_progress(migration, checkpoint.rank,
                              active->data_total_bytes);
    if (committed_ranks_[migration].size() == checkpoint.gang_size) {
      p3_->release_migration(migration);
      for (auto &plan : checkpoint_plans_[checkpoint.job_id]) {
        plan.migration_id.clear();
        plan.checkpoint_id.clear();
      }
    } else
      apply_batch_locked(p3_->admit_batch({}, checkpoint.completed_at_ns),
                         checkpoint.completed_at_ns);
  }
  if (restored && completed_ranks_[migration].size() == checkpoint.gang_size) {
    auto destinations = migration_destinations_.find(checkpoint.job_id);
    if (destinations != migration_destinations_.end() &&
        destinations->second.size() == checkpoint.gang_size)
      if (auto resumed = queue_resume_locked(checkpoint.job_id, migration,
                                             destinations->second);
          !resumed)
        return resumed;
  }
  return ok_void();
}
VoidResult Coordinator::on_migration_progress(const JobId &id, uint64_t bytes,
                                              bool completed,
                                              const std::string &migration_id,
                                              uint32_t rank, bool failed,
                                              int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  auto job = job_reg_.get(id);
  if (!job)
    return err_void("Unknown migration job");
  auto active = p3_->get_reservation(id, rank, migration_id);
  if (!active) {
    if (!migration_id.empty() && completed_ranks_[migration_id].count(rank))
      return ok_void();
    return err_void("No matching active migration rank");
  }
  const auto migration = active->migration_id;
  if (completed && !failed && completed_ranks_[migration].count(rank))
    return ok_void();
  if (stamp <= 0)
    stamp = now_ns();
  if (failed) {
    p3_->release_migration(migration);
    auto recovery = recovery_locked(*job, sources_of(*job), stamp);
    return recovery ? ok_void() : VoidResult(recovery.error());
  }
  if (completed && bytes < active->data_total_bytes)
    return err_void("Migration completion requires the entire payload");
  if (completed && stamp > active->deadline_ns) {
    auto recovery = recovery_locked(*job, sources_of(*job), stamp);
    return recovery ? ok_void()
                    : err_void("Migration completed after its deadline; "
                               "recovery remains queued");
  }
  p3_->update_rank_progress(migration, rank, bytes);
  if (!completed)
    return ok_void();
  if (!runtime_specs_[id].empty() && !restored_ranks_[migration].count(rank))
    return err_void("Destination has not confirmed restore");
  completed_ranks_[migration].insert(rank);
#if RECLAIMNET_HAVE_JSON
  trace_locked("events.jsonl", J{{"type", "MIGRATION_SOURCE_STOPPED"},
                                 {"node_id", active->src},
                                 {"source_node_id", active->src},
                                 {"job_id", id},
                                 {"barrier_id", migration},
                                 {"migration_id", migration},
                                 {"rank", rank},
                                 {"timestamp_ns", stamp}}
                                   .dump());
  if (auto saved = store_->set_runtime(
          "completed_migration", migration,
          J{{"job_id", id}, {"ranks", completed_ranks_[migration]}}.dump());
      !saved)
    return saved;
#endif
  if (completed_ranks_[migration].size() < active->gang_size)
    return ok_void();
  const auto destinations = migration_destinations_[id];
  if (destinations.size() != job->rank_count)
    return err_void("Missing committed gang destination");
  std::vector<MigrationEntry> completed_entries;
  for (const auto &entry : p3_->active_migrations())
    if (entry.migration_id == migration)
      completed_entries.push_back(entry);
  job_reg_.update(id, [&](JobRecord &record) {
    record.rank_nodes = destinations;
    record.node_id = destinations.front();
    record.migration_dst.clear();
    record.state = JobState::RUNNING;
    record.last_update_ns = now_ns();
  });
  persist_job_locked(id);
  p3_->release_migration(migration);
  store_->erase_runtime("migration_target", id);
  migration_sessions_.erase(id);
  if (auto queued = queue_resume_locked(id, migration, destinations); !queued)
    return queued;
  for (const auto &source : sources_of(*job)) {
    const auto duration =
        std::max(0.0, static_cast<double>(stamp - active->start_ns) / 1e9);
    p2_->observe_migration_cost(source, duration, stamp);
#if RECLAIMNET_HAVE_JSON
    const auto sample = J{
        {"node_id", source},
        {"duration_s", duration},
        {"completed_ns",
         stamp}}.dump();
    if (auto saved = store_->set_runtime("migration_cost",
                                         source + ":" + migration, sample);
        !saved)
      return saved;
    trace_locked("historical_migration_profiles.jsonl", sample);
#endif
  }
#if RECLAIMNET_HAVE_JSON
  J paths = J::array();
  J destinations_json = destinations;
  for (const auto &replica : catalog_.all())
    if (replica.job_id == id && replica.checkpoint_id == migration)
      paths.push_back({{"rank", replica.rank},
                       {"dst_node_id", replica.replica_node_id},
                       {"bytes", replica.size_bytes}});
  trace_locked("events.jsonl", J{{"type", "MIGRATION_DONE"},
                                 {"job_id", id},
                                 {"migration_id", migration},
                                 {"timestamp_ns", stamp},
                                 {"destination_node_ids", destinations_json},
                                 {"committed_ranks", paths}}
                                   .dump());
  J allocations = J::array();
  uint64_t total = 0;
  auto level = TopologyLevel::INTRA_RACK;
  for (const auto &entry : completed_entries) {
    J edges = J::array();
    for (size_t i = 1; i < entry.path_nodes.size(); ++i)
      edges.push_back({entry.path_nodes[i - 1], entry.path_nodes[i]});
    allocations.push_back({{"rank", entry.rank},
                           {"src_node_id", entry.src},
                           {"dst_node_id", entry.dst},
                           {"bytes", entry.data_total_bytes},
                           {"required_bps", entry.minimum_required_bps},
                           {"allocated_bps", entry.allocated_bps},
                           {"allocation_version", entry.allocation_version},
                           {"path_edges", edges}});
    total += entry.data_total_bytes;
    if (auto path = topo_->shortest_path(entry.src, entry.dst))
      level = std::max(level, path->max_level);
  }
  const char *levels[] = {"IntraRack", "IntraBuilding", "CrossBuilding",
                          "CoreCrossing"};
  trace_locked(
      "migrations.jsonl",
      J{{"run_id", cfg_.run_id},
        {"migration_id", migration},
        {"job_id", id},
        {"src_node_id", job->node_id},
        {"dst_node_id", destinations.front()},
        {"reclaim_type", "SCHEDULED"},
        {"phase", "COMPLETED"},
        {"topology_level", levels[static_cast<unsigned>(level)]},
        {"p3_applicable", true},
        {"p3_status", "COMPLETED"},
        {"checkpoint_size_bytes", total},
        {"bytes_to_transfer_total", total},
        {"deadline_s", active->deadline_s},
        {"deadline_ns", active->deadline_ns},
        {"decision_ns", active->start_ns},
        {"start_ns", active->start_ns},
        {"end_ns", stamp},
        {"restore_time_s", active->restore_time_s},
        {"available_transfer_s", active->deadline_s - active->restore_time_s},
        {"path_allocations", allocations},
        {"allocation_method", "EDF_THEN_MAX_MIN_WATER_FILL"}}
          .dump());
#endif
  return store_->log_event("MIGRATION_DONE", "{}", stamp);
}
VoidResult Coordinator::recovery_complete(const NodeId &node, const JobId &id,
                                          const std::string &checkpoint_id,
                                          uint32_t rank, int64_t stamp) {
  std::lock_guard<std::mutex> lock(control_mu_);
  if (!running_)
    return err_void("Coordinator is not running");
  const auto ack = std::make_tuple(node, id, checkpoint_id, rank);
  if (recovery_acks_.count(ack) && !recovery_queue_.count(id))
    return ok_void();
  auto barrier = recovery_barriers_.find(id);
  if (barrier != recovery_barriers_.end() && !barrier->second.waiting.empty())
    return err_void("Recovery must wait for surviving source ranks to stop");
  auto recovery = recovery_queue_.find(id);
  if (recovery == recovery_queue_.end())
    return err_void("No queued recovery");
  auto checkpoint = std::find_if(
      recovery->second.begin(), recovery->second.end(), [&](const auto &value) {
        return value.rank == rank && value.checkpoint_id == checkpoint_id &&
               value.replica_node_id == node;
      });
  if (checkpoint == recovery->second.end())
    return err_void(
        "Recovery acknowledgement does not match the selected checkpoint");
  recovery_acks_.insert(ack);
  restored_ranks_[checkpoint_id].insert(rank);
#if RECLAIMNET_HAVE_JSON
  if (auto saved = store_->set_runtime(
          "restored_rank", checkpoint_id + ":" + std::to_string(rank),
          J{{"job_id", id}, {"migration_id", checkpoint_id}, {"rank", rank}}
              .dump());
      !saved)
    return saved;
  if (auto saved = store_->set_runtime("recovery_ack",
                                       node + ":" + id + ":" + checkpoint_id +
                                           ":" + std::to_string(rank),
                                       J{{"node_id", node},
                                         {"job_id", id},
                                         {"checkpoint_id", checkpoint_id},
                                         {"rank", rank},
                                         {"sequence", checkpoint->sequence}}
                                           .dump());
      !saved)
    return saved;
#endif
  const std::string key =
      "recovery:" + id + ":" + std::to_string(checkpoint->sequence);
  completed_ranks_[key].insert(rank);
  if (completed_ranks_[key].size() < checkpoint->gang_size)
    return ok_void();
  std::vector<NodeId> destinations(checkpoint->gang_size);
  for (const auto &selected : recovery->second)
    destinations[selected.rank] = selected.replica_node_id;
  job_reg_.update(id, [&](JobRecord &record) {
    record.rank_nodes = destinations;
    record.node_id = destinations.front();
    record.state = JobState::RUNNING;
    record.migration_dst.clear();
    record.last_update_ns = stamp > 0 ? stamp : now_ns();
  });
  persist_job_locked(id);
  recovery_queue_.erase(recovery);
  recovery_barriers_.erase(id);
  store_->erase_runtime("migration_target", id);
  migration_sessions_.erase(id);
  if (auto queued = queue_resume_locked(id, key, destinations); !queued)
    return queued;
#if RECLAIMNET_HAVE_JSON
  trace_locked("events.jsonl", J{{"type", "RECOVERY_DONE"},
                                 {"job_id", id},
                                 {"checkpoint_id", checkpoint_id},
                                 {"timestamp_ns", stamp > 0 ? stamp : now_ns()},
                                 {"destination_node_ids", destinations}}
                                   .dump());
#endif
  return store_->log_event("RECOVERY_DONE", "{}", stamp);
}

}
