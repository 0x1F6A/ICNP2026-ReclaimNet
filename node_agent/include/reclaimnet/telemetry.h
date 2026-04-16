#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <functional>
#include <thread>
#include <atomic>
#include <string>

namespace reclaimnet {
namespace agent {


struct LocalTelemetry {
  double   gpu_util_pct;
  double   vram_used_pct;
  uint64_t vram_free_bytes;
  double   link_tx_bps;
  double   link_rx_bps;
  double   cpu_load_1m;
  int64_t  sampled_ns;
};


using TelemetryCb = std::function<void(const LocalTelemetry&)>;


class TelemetryCollector {
public:
  TelemetryCollector(std::string iface, double interval_s);
  ~TelemetryCollector();


  void start(TelemetryCb cb);


  void stop();


  LocalTelemetry sample_now() const;

  bool running() const noexcept { return running_.load(); }

private:
  std::string iface_;
  double      interval_s_;
  TelemetryCb cb_;
  std::atomic<bool> running_{false};
  std::thread       thread_;

  void run_loop();
  LocalTelemetry collect() const;
};

}
}
