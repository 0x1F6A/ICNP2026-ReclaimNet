


#include "reclaimnet/coordinator.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <chrono>
#include <thread>
#include <ctime>

namespace reclaimnet {
namespace control {



Coordinator::Coordinator(CoordinatorConfig cfg)
  : cfg_(std::move(cfg)),
    hazard_(cfg_.hazard_decay_s),
    p3_(std::make_unique<P3Admission>(cfg_.migration_share)) {}

Coordinator::~Coordinator() { stop(); }



VoidResult Coordinator::start() {
  LOG_INFO("Coordinator starting (listen=%s)", cfg_.listen_address.c_str());


  topo_ = std::make_unique<TopologyGraph>();
  if (!cfg_.topology_file.empty()) {
    auto r = TopologyGraph::load_from_file(cfg_.topology_file);
    if (r) {
      *topo_ = std::move(r.value());
      LOG_INFO("Topology loaded: %zu nodes", topo_->node_count());
    } else {
      LOG_WARN("Topology load failed: %s – using empty graph", r.error().msg.c_str());
    }
  }


  store_ = std::make_unique<PersistenceStore>(cfg_.sqlite_path);
  if (auto res = store_->init_schema(); !res) {
    LOG_WARN("DB schema init failed: %s", res.error().msg.c_str());
  }


  if (auto jobs = store_->load_all_jobs(); jobs) {
    for (auto& j : jobs.value()) job_reg_.add(j);
    LOG_INFO("Restored %zu jobs from DB", jobs.value().size());
  }
  if (auto nodes = store_->load_all_nodes(); nodes) {
    for (auto& n : nodes.value()) node_reg_.upsert(n);
    LOG_INFO("Restored %zu nodes from DB", nodes.value().size());
  }


  p1_ = std::make_unique<P1Allocator>(static_cast<double>(cfg_.B_ckpt_bps));
  p2_ = std::make_unique<P2Selector>(cfg_.alpha, cfg_.beta, cfg_.gamma,
                                     *topo_, hazard_, node_reg_);


  for (const NodeId& src : topo_->all_nodes()) {
    for (const Link& lk : topo_->links_from(src)) {
      p3_->add_link(lk.src, lk.dst, lk.bandwidth_bps);
    }
  }

  running_ = true;
  epoch_thread_ = std::thread([this] { run_epoch_loop(); });
  LOG_INFO("Coordinator started");
  return ok_void();
}



void Coordinator::stop() {
  if (running_.exchange(false)) {
    if (epoch_thread_.joinable()) epoch_thread_.join();
    LOG_INFO("Coordinator stopped");
  }
}



void Coordinator::run_epoch_loop() {
  while (running_.load()) {
    run_one_epoch();
    auto ms = static_cast<long>(cfg_.epoch_seconds * 1000.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }
}



void Coordinator::run_one_epoch() {
  LOG_DEBUG("Coordinator: epoch start");


  int64_t stale_ns = now_ns() -
      static_cast<int64_t>(cfg_.heartbeat_timeout_s * 1e9);
  auto stale = node_reg_.mark_stale(stale_ns);
  for (auto& nid : stale) {
    LOG_WARN("Coordinator: node %s went stale", nid.c_str());
    store_->log_event("NODE_STALE", "{\"node_id\":\"" + nid + "\"}");
  }


  auto running_jobs = job_reg_.by_state(JobState::RUNNING);
  std::vector<P1JobInput> p1_in;
  for (auto& j : running_jobs) {
    P1JobInput inp;
    inp.job_id             = j.job_id;
    inp.lambda_i           = std::max(j.lambda_i, hazard_.lambda(j.node_id));
    inp.checkpoint_size_b  = j.checkpoint_size;
    inp.write_cap_bps      = 0.0;
    p1_in.push_back(inp);
  }
  if (!p1_in.empty()) {
    auto epoch_res = p1_->compute(p1_in);
    LOG_DEBUG("Coordinator P1: %zu jobs scheduled, total_bps=%.0f",
              epoch_res.schedules.size(), epoch_res.total_bps_used);


    for (auto& sched : epoch_res.schedules) {
      job_reg_.update(sched.job_id, [&](JobRecord& r) {
        r.lambda_i = sched.b_i_bps;
      });
    }
  }


  double now_s = static_cast<double>(std::time(nullptr));
  auto expired = p3_->expired_migrations(now_s);
  for (auto& e : expired) {
    LOG_WARN("Coordinator P3: migration expired for job %s", e.job_id.c_str());
    p3_->release(e.job_id);
    store_->log_event("MIGRATION_EXPIRED", "{\"job_id\":\"" + e.job_id + "\"}");
  }

  LOG_DEBUG("Coordinator: epoch done");
}



void Coordinator::on_heartbeat(const NodeId& node_id, const NodeTelemetry& tel) {
  node_reg_.heartbeat(node_id, now_ns());
  node_reg_.update_telemetry(node_id, tel);
  LOG_DEBUG("Coordinator: heartbeat from %s", node_id.c_str());
}



Result<MigrationCommand> Coordinator::on_reclaim_signal(
    const NodeId& src_node, const JobId& job_id,
    double deadline_s, ReclaimType type) {

  LOG_INFO("Coordinator: reclaim signal node=%s job=%s type=%d deadline=%.1fs",
           src_node.c_str(), job_id.c_str(), (int)type, deadline_s);


  hazard_.record_event({src_node, now_ns(), type});
  store_->log_event("RECLAIM_SIGNAL",
    "{\"node\":\"" + src_node + "\",\"job\":\"" + job_id + "\"}");


  auto opt_job = job_reg_.get(job_id);
  if (!opt_job) return Error{"Job not found: " + job_id};
  const JobRecord& job = opt_job.value();


  P2Query q;
  q.job_id             = job_id;
  q.source_node        = src_node;
  q.checkpoint_size_b  = job.checkpoint_size;
  q.reclaim_deadline_s = deadline_s;
  q.T_restore_s        = 5.0;
  auto p2_res = p2_->select(q);
  if (!p2_res.success) {
    return Error{"P2 failed: " + p2_res.reason};
  }


  MigrationEntry entry;
  entry.job_id               = job_id;
  entry.src                  = src_node;
  entry.dst                  = p2_res.selected;
  entry.data_remaining_bytes = job.checkpoint_size;
  entry.deadline_s           = deadline_s;
  entry.allocated_bps        = 0.0;
  entry.start_ns             = now_ns();
  entry.emergency            = (type == ReclaimType::EMERGENCY);

  auto admit_res = p3_->admit(entry);
  if (!admit_res) {
    LOG_WARN("Coordinator P3 admission denied: %s", admit_res.error().msg.c_str());

    if (type != ReclaimType::EMERGENCY)
      return Error{"P3 admission denied: " + admit_res.error().msg};
  }


  job_reg_.update(job_id, [&](JobRecord& r) {
    r.state         = JobState::MIGRATING;
    r.migration_dst = p2_res.selected;
  });
  store_->log_event("MIGRATION_START",
    "{\"job\":\"" + job_id + "\",\"dst\":\"" + p2_res.selected + "\"}");

  MigrationCommand cmd;
  cmd.job_id        = job_id;
  cmd.dst_node      = p2_res.selected;
  cmd.allocated_bps = entry.allocated_bps;
  cmd.deadline_s    = deadline_s;
  return cmd;
}



void Coordinator::on_migration_progress(
    const JobId& job_id, uint64_t bytes_done, bool completed) {
  p3_->update_progress(job_id, bytes_done);
  if (completed) {
    p3_->release(job_id);
    job_reg_.update(job_id, [](JobRecord& r) {
      r.state = JobState::RUNNING;
    });
    store_->log_event("MIGRATION_DONE", "{\"job\":\"" + job_id + "\"}");
    LOG_INFO("Coordinator: migration complete for job %s", job_id.c_str());
  }
}



VoidResult Coordinator::register_node(const NodeRecord& rec) {
  node_reg_.upsert(rec);
  return store_->upsert_node(rec);
}

VoidResult Coordinator::register_job(const JobRecord& rec) {
  auto r = job_reg_.add(rec);
  if (!r) return r;
  return store_->upsert_job(rec);
}

}
}
