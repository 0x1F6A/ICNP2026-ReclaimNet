


#include "reclaimnet/agent.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <thread>
#include <chrono>

namespace reclaimnet {
namespace agent {

NodeAgent::NodeAgent(AgentConfig cfg) : cfg_(std::move(cfg)) {}

NodeAgent::~NodeAgent() { stop(); }

VoidResult NodeAgent::start() {
  LOG_INFO("NodeAgent: starting (node_id=%s)", cfg_.node_id.c_str());


  ckpt_mgr_   = std::make_unique<CheckpointManager>(cfg_.checkpoint_dir);
  mig_client_ = std::make_unique<MigrationClient>();
  telemetry_  = std::make_unique<TelemetryCollector>(cfg_.iface,
                                                      cfg_.telemetry_interval_s);
  grpc_client_= std::make_unique<CoordinatorClient>(
      cfg_.coordinator_address, cfg_.mtls_enabled,
      cfg_.tls_cert_path, cfg_.tls_key_path, cfg_.tls_ca_path);


  auto signal_cb = [this](const ReclaimNotification& notif) -> Result<ReclaimResponse> {
    return grpc_client_->reclaim_signal(notif, cfg_.node_id);
  };
  reclaim_handler_ = std::make_unique<ReclaimHandler>(*ckpt_mgr_, *mig_client_,
                                                       std::move(signal_cb));


  {
    auto r = ebpf_load_tc_ratelimit("data_plane/tc_ratelimit.bpf.o", cfg_.iface);
    if (r) {
      ebpf_tc_ = r.value();
      LOG_INFO("NodeAgent: eBPF tc_ratelimit loaded");
    } else {
      LOG_WARN("NodeAgent: eBPF load skipped (%s)", r.error().msg.c_str());
    }
  }


  auto local_tel = telemetry_->sample_now();
  if (auto r = grpc_client_->register_node(
          cfg_.node_id, cfg_.listen_address,
          cfg_.building, cfg_.switch_id,
          cfg_.total_vram_bytes, cfg_.link_capacity_bps); !r) {
    LOG_WARN("NodeAgent: register_node failed: %s", r.error().msg.c_str());
  }


  telemetry_->start([this](const LocalTelemetry& tel) {
    if (auto r = grpc_client_->heartbeat(cfg_.node_id, tel); !r) {
      LOG_WARN("NodeAgent: telemetry heartbeat failed: %s", r.error().msg.c_str());
    }
  });

  running_ = true;
  heartbeat_thread_ = std::thread([this] { run_heartbeat_loop(); });

  LOG_INFO("NodeAgent: started");
  return ok_void();
}

void NodeAgent::stop() {
  if (running_.exchange(false)) {
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    if (telemetry_) telemetry_->stop();
    ebpf_unload_all(cfg_.iface);
    LOG_INFO("NodeAgent: stopped");
  }
}

void NodeAgent::run_heartbeat_loop() {


  while (running_.load()) {
    auto ms = static_cast<long>(cfg_.heartbeat_interval_s * 1000.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    auto tel = telemetry_->sample_now();
    if (auto r = grpc_client_->heartbeat(cfg_.node_id, tel); !r) {
      LOG_WARN("NodeAgent: heartbeat failed: %s", r.error().msg.c_str());
    }
  }
}

VoidResult NodeAgent::handle_reclaim(const ReclaimNotification& notif) {
  return reclaim_handler_->handle(notif);
}

}
}
