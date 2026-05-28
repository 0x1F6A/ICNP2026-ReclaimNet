

#include "reclaimnet/telemetry.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>
#include <thread>

namespace reclaimnet {
namespace agent {

TelemetryCollector::TelemetryCollector(std::string iface, double interval_s,
                                       std::shared_ptr<GpuBackend> gpu_backend)
    : iface_(std::move(iface)), interval_s_(interval_s),
      gpu_(std::move(gpu_backend)) {}

TelemetryCollector::~TelemetryCollector() { stop(); }

void TelemetryCollector::start(TelemetryCb cb) {
  cb_ = std::move(cb);
  running_ = true;
  thread_ = std::thread([this] { run_loop(); });
}

void TelemetryCollector::stop() {
  if (running_.exchange(false) && thread_.joinable())
    thread_.join();
}

void TelemetryCollector::run_loop() {
  while (running_.load()) {
    if (cb_)
      cb_(collect());
    auto ms = static_cast<long>(interval_s_ * 1000.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }
}

LocalTelemetry TelemetryCollector::collect() const {
  std::lock_guard<std::mutex> lk(sample_mu_);
  LocalTelemetry t{};
  t.sampled_ns = now_ns();

#ifdef __linux__
  {
    std::ifstream ifs("/proc/loadavg");
    if (ifs)
      ifs >> t.cpu_load_1m;
  }

  {
    std::ifstream ifs("/proc/net/dev");
    std::string line;
    while (std::getline(ifs, line)) {
      auto colon = line.find(':');
      if (colon == std::string::npos)
        continue;
      std::string iface = line.substr(0, colon);
      const auto first = iface.find_first_not_of(" \t");
      if (first == std::string::npos)
        continue;
      iface = iface.substr(first, iface.find_last_not_of(" \t") - first + 1);
      if (iface != iface_)
        continue;
      std::istringstream ss(line.substr(colon + 1));
      uint64_t rx_bytes, rx_pkts, rx_errs, rx_drop, rx_fifo, rx_frame, rx_comp,
          rx_mcast, tx_bytes;
      if (!(ss >> rx_bytes >> rx_pkts >> rx_errs >> rx_drop >> rx_fifo >>
            rx_frame >> rx_comp >> rx_mcast >> tx_bytes))
        break;

      const int64_t sample_mono_ns = mono_ns();
      if (previous_network_mono_ns_ > 0 &&
          sample_mono_ns > previous_network_mono_ns_) {
        const double elapsed_s =
            static_cast<double>(sample_mono_ns - previous_network_mono_ns_) /
            1e9;
        t.link_rx_bps =
            rx_bytes >= previous_rx_bytes_
                ? bytes_to_bits(rx_bytes - previous_rx_bytes_) / elapsed_s
                : 0.0;
        t.link_tx_bps =
            tx_bytes >= previous_tx_bytes_
                ? bytes_to_bits(tx_bytes - previous_tx_bytes_) / elapsed_s
                : 0.0;
      }
      previous_rx_bytes_ = rx_bytes;
      previous_tx_bytes_ = tx_bytes;
      previous_network_mono_ns_ = sample_mono_ns;
      break;
    }
  }

#else
  t.link_rx_bps = 0.0;
  t.link_tx_bps = 0.0;
  t.cpu_load_1m = 0.0;
#endif
  auto devices = gpu_.inventory();
  if (devices) {
    t.gpu_count = static_cast<uint32_t>(devices.value().size());
    for (const auto &device : devices.value()) {
      t.total_vram_bytes += device.total_vram_bytes;
      t.vram_free_bytes += device.free_vram_bytes;
      t.gpu_util_pct += device.utilization_pct;
      if (!t.cuda_capability || device.cuda_capability < t.cuda_capability)
        t.cuda_capability = device.cuda_capability;
      if (device.utilization_pct < 5 &&
          device.free_vram_bytes >= device.total_vram_bytes * 0.9)
        ++t.gpu_free_count;
    }
    if (t.gpu_count)
      t.gpu_util_pct /= t.gpu_count;
    if (t.total_vram_bytes)
      t.vram_used_pct =
          100.0 * (t.total_vram_bytes - t.vram_free_bytes) / t.total_vram_bytes;
  }
  return t;
}

LocalTelemetry TelemetryCollector::sample_now() const { return collect(); }

}
}
