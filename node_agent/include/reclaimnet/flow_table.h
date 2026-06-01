#pragma once

#include "reclaimnet/types.h"

#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace reclaimnet {
namespace agent {

struct FlowStats {
  FiveTuple key;
  uint64_t bytes_tx;
  uint64_t bytes_rx;
  uint64_t packets_tx;
  int64_t first_seen_ns;
  int64_t last_seen_ns;
  double rate_bps;
};

class FlowTable {
public:
  FlowTable() = default;

  void record_tx(const FiveTuple &key, uint64_t bytes, int64_t now_ns);

  void record_rx(const FiveTuple &key, uint64_t bytes, int64_t now_ns);

  void remove(const FiveTuple &key);

  std::vector<FlowStats> snapshot() const;

  std::optional<FlowStats> get(const FiveTuple &key) const;

  size_t size() const;

private:
  mutable std::mutex mu_;
  std::unordered_map<FiveTuple, FlowStats> table_;

  void update_rate(FlowStats &fs, uint64_t new_bytes, int64_t now_ns);
};

}
}
