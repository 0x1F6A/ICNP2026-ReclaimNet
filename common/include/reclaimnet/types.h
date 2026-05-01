

#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

namespace reclaimnet {

using JobId = std::string;

using NodeId = std::string;

using BandwidthBps = uint64_t;

inline double bytes_to_bits(uint64_t bytes) noexcept {
  return static_cast<double>(bytes) * 8.0;
}

using Duration = std::chrono::nanoseconds;

using TimePoint = std::chrono::steady_clock::time_point;

using WallClock = std::chrono::system_clock::time_point;

struct FiveTuple {
  uint32_t src_ip = 0;
  uint32_t dst_ip = 0;
  uint16_t src_port = 0;
  uint16_t dst_port = 0;
  uint8_t protocol = 0;

  bool operator==(const FiveTuple &o) const noexcept {
    return src_ip == o.src_ip && dst_ip == o.dst_ip && src_port == o.src_port &&
           dst_port == o.dst_port && protocol == o.protocol;
  }
  bool operator!=(const FiveTuple &o) const noexcept { return !(*this == o); }
};

enum class NodeState : uint8_t {
  UNKNOWN = 0,
  ONLINE = 1,
  DEGRADED = 2,
  OFFLINE = 3,
  DRAINING = 4,
};

enum class JobState : uint8_t {
  UNKNOWN = 0,
  RUNNING = 1,
  CHECKPOINTING = 2,
  MIGRATING = 3,
  SUSPENDED = 4,
  COMPLETED = 5,
  FAILED = 6,
};

enum class ReclaimType : uint8_t {
  SCHEDULED = 0,
  EMERGENCY = 1,
};

enum class TopologyLevel : uint8_t {
  INTRA_RACK = 0,
  INTRA_BUILDING = 1,
  CROSS_BUILDING = 2,
  CORE_CROSSING = 3,
};

enum class MigrationPhase : uint8_t {
  IDLE = 0,
  ADMITTED = 1,
  TRANSFERRING = 2,
  RESTORING = 3,
  COMPLETED = 4,
  ABORTED = 5,
};

constexpr double kDefaultMigrationShare = 0.70;

constexpr double kSurvivalHorizonSeconds = 3600.0;

constexpr double kBurstFactor = 0.050;

constexpr Duration kMinCheckpointInterval = std::chrono::seconds(1);

constexpr Duration kMaxCheckpointInterval = std::chrono::minutes(10);

constexpr const char *kProtocolVersion = "1.0.0";

}

namespace std {
template <> struct hash<reclaimnet::FiveTuple> {
  size_t operator()(const reclaimnet::FiveTuple &ft) const noexcept {
    size_t h = 0;
    auto mix = [&](size_t v) { h ^= v + 0x9e3779b9 + (h << 6) + (h >> 2); };
    mix(std::hash<uint32_t>{}(ft.src_ip));
    mix(std::hash<uint32_t>{}(ft.dst_ip));
    mix(std::hash<uint32_t>{}((uint32_t(ft.src_port) << 16) | ft.dst_port));
    mix(std::hash<uint8_t>{}(ft.protocol));
    return h;
  }
};
}
