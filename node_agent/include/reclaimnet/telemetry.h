#pragma once

#include "reclaimnet/gpu_isolation.h"
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace reclaimnet {
namespace agent {

struct LocalTelemetry {
  double gpu_util_pct;
  double vram_used_pct;
  uint64_t vram_free_bytes;
  double link_tx_bps;
  double link_rx_bps;
  double cpu_load_1m;
  int64_t sampled_ns;
  uint32_t gpu_count = 0;
  uint32_t gpu_free_count = 0;
  double cuda_capability = 0;
  uint64_t total_vram_bytes = 0;
};

using TelemetryCb = std::function<void(const LocalTelemetry &)>;

class TelemetryCollector {
public:
  TelemetryCollector(std::string iface, double interval_s,
                     std::shared_ptr<GpuBackend> gpu_backend = {});
  ~TelemetryCollector();

  void start(TelemetryCb cb);

  void stop();

  LocalTelemetry sample_now() const;

  bool running() const noexcept { return running_.load(); }

private:
  std::string iface_;
  double interval_s_;
  TelemetryCb cb_;
  std::atomic<bool> running_{false};
  std::thread thread_;
  mutable std::mutex sample_mu_;
#ifdef __linux__
  mutable uint64_t previous_rx_bytes_ = 0;
  mutable uint64_t previous_tx_bytes_ = 0;
  mutable int64_t previous_network_mono_ns_ = 0;
#endif
  mutable GpuIsolation gpu_;

  void run_loop();
  LocalTelemetry collect() const;
};

}
}
