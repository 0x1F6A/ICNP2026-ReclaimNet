

#include "reclaimnet/flow_table.h"
#include "reclaimnet/time.h"

#include <algorithm>
#include <cmath>

namespace reclaimnet {
namespace agent {

static constexpr double ALPHA_PER_SEC = 0.693147;

void FlowTable::update_rate(FlowStats &fs, uint64_t new_bytes,
                            int64_t now_ns_val) {
  double dt_s = static_cast<double>(now_ns_val - fs.last_seen_ns) / 1e9;
  if (dt_s <= 0.0)
    dt_s = 1e-6;
  double inst_rate = (double)new_bytes * 8.0 / dt_s;
  double alpha = std::exp(-ALPHA_PER_SEC * dt_s);
  fs.rate_bps = alpha * fs.rate_bps + (1.0 - alpha) * inst_rate;
  fs.last_seen_ns = now_ns_val;
}

void FlowTable::record_tx(const FiveTuple &key, uint64_t bytes,
                          int64_t now_ns_val) {
  std::lock_guard<std::mutex> lk(mu_);
  auto &fs = table_[key];
  if (fs.first_seen_ns == 0) {
    fs.key = key;
    fs.first_seen_ns = now_ns_val;
    fs.last_seen_ns = now_ns_val;
  }
  fs.bytes_tx += bytes;
  fs.packets_tx++;
  update_rate(fs, bytes, now_ns_val);
}

void FlowTable::record_rx(const FiveTuple &key, uint64_t bytes,
                          int64_t now_ns_val) {
  std::lock_guard<std::mutex> lk(mu_);
  auto &fs = table_[key];
  if (fs.first_seen_ns == 0) {
    fs.key = key;
    fs.first_seen_ns = now_ns_val;
    fs.last_seen_ns = now_ns_val;
  }
  fs.bytes_rx += bytes;
  update_rate(fs, bytes, now_ns_val);
}

void FlowTable::remove(const FiveTuple &key) {
  std::lock_guard<std::mutex> lk(mu_);
  table_.erase(key);
}

std::vector<FlowStats> FlowTable::snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<FlowStats> out;
  out.reserve(table_.size());
  for (auto &[_, fs] : table_)
    out.push_back(fs);
  return out;
}

std::optional<FlowStats> FlowTable::get(const FiveTuple &key) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = table_.find(key);
  if (it == table_.end())
    return std::nullopt;
  return it->second;
}

size_t FlowTable::size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return table_.size();
}

}
}
