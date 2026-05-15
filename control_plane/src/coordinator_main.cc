

#include "reclaimnet/config.h"
#include "reclaimnet/coordinator.h"
#include "reclaimnet/grpc_service.h"
#include "reclaimnet/log.h"
#include "reclaimnet/metrics_server.h"

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <thread>

namespace {
std::atomic<bool> g_shutdown{false};

void sig_handler(int) { g_shutdown = true; }
}

int main(int argc, char *argv[]) {
  using namespace reclaimnet;
  using namespace reclaimnet::control;

  const char *cfg_path = (argc > 1) ? argv[1] : "configs/coordinator.yaml";
  auto cfg_res = load_coordinator_config(cfg_path);
  if (!cfg_res) {
    LOG_FATAL("Cannot load config from %s: %s", cfg_path,
              cfg_res.error().message.c_str());
    return 1;
  }
  auto cfg = cfg_res.value();
  if (auto v = validate_coordinator_config(cfg); !v) {
    LOG_FATAL("Invalid config: %s", v.error().message.c_str());
    return 1;
  }

  Logger::instance().set_level(LogLevel::INFO);
  LOG_INFO("ReclaimNet coordinator starting");

  Coordinator coord(cfg);
  if (auto r = coord.start(); !r) {
    LOG_FATAL("Coordinator start failed: %s", r.error().message.c_str());
    return 1;
  }

  MetricsServer metrics;
  if (auto result = metrics.start(cfg.prometheus_port); !result) {
    LOG_FATAL("Metrics: %s", result.error().message.c_str());
    return 1;
  }

  std::signal(SIGINT, sig_handler);
  std::signal(SIGTERM, sig_handler);

  std::thread grpc_thread([&] {
    auto r =
        run_grpc_server(coord, cfg.listen_address, cfg.mtls_enabled,
                        cfg.tls_cert_path, cfg.tls_key_path, cfg.tls_ca_path);
    if (!r) {
      LOG_ERR("gRPC server error: %s", r.error().message.c_str());
      g_shutdown = true;
    }
  });

  while (!g_shutdown.load())
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

  LOG_INFO("Shutdown signal received");
  coord.stop();
  grpc_thread.join();
  metrics.stop();
  return 0;
}
