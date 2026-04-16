


#include "reclaimnet/telemetry.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <fstream>
#include <sstream>
#include <cstring>
#include <thread>
#include <chrono>

namespace reclaimnet {
namespace agent {

TelemetryCollector::TelemetryCollector(std::string iface, double interval_s)
  : iface_(std::move(iface)), interval_s_(interval_s) {}

TelemetryCollector::~TelemetryCollector() { stop(); }

void TelemetryCollector::start(TelemetryCb cb) {
  cb_      = std::move(cb);
  running_ = true;
  thread_  = std::thread([this] { run_loop(); });
}

void TelemetryCollector::stop() {
  if (running_.exchange(false) && thread_.joinable())
    thread_.join();
}

void TelemetryCollector::run_loop() {
  while (running_.load()) {
    if (cb_) cb_(collect());
    auto ms = static_cast<long>(interval_s_ * 1000.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }
}



LocalTelemetry TelemetryCollector::collect() const {
  LocalTelemetry t{};
  t.sampled_ns = now_ns();


#ifdef __linux__
  {
    std::ifstream ifs("/proc/loadavg");
    if (ifs) ifs >> t.cpu_load_1m;
  }


  {
    std::ifstream ifs("/proc/net/dev");
    std::string line;
    while (std::getline(ifs, line)) {
      if (line.find(iface_) == std::string::npos) continue;

      auto colon = line.find(':');
      if (colon == std::string::npos) continue;
      std::istringstream ss(line.substr(colon + 1));
      uint64_t rx_bytes, rx_pkts, rx_errs, rx_drop, rx_fifo, rx_frame,
               rx_comp, rx_mcast,
               tx_bytes;
      ss >> rx_bytes >> rx_pkts >> rx_errs >> rx_drop
         >> rx_fifo >> rx_frame >> rx_comp >> rx_mcast
         >> tx_bytes;


      t.link_rx_bps = (double)rx_bytes;
      t.link_tx_bps = (double)tx_bytes;
    }
  }



  t.gpu_util_pct  = 0.0;
  t.vram_used_pct = 0.0;
  t.vram_free_bytes = 8ULL * 1024 * 1024 * 1024;
#else

  t.gpu_util_pct    = 0.0;
  t.vram_used_pct   = 0.0;
  t.vram_free_bytes = 8ULL * 1024 * 1024 * 1024;
  t.link_rx_bps     = 0.0;
  t.link_tx_bps     = 0.0;
  t.cpu_load_1m     = 0.0;
#endif

  return t;
}

LocalTelemetry TelemetryCollector::sample_now() const { return collect(); }

}
}
