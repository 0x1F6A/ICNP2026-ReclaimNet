/// @file types.h
/// @brief Core type aliases and enumerations used throughout ReclaimNet.
///
/// Defines the fundamental types referenced by all three protocols:
///   - P1 (Reclaim-Aware Checkpoint Scheduling)
///   - P2 (Volatility-Aware Destination Selection)
///   - P3 (Migration Traffic Control)

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <array>
#include <functional>

namespace reclaimnet {

// ── Type aliases ──────────────────────────────────────────────────────────────

/// Opaque job identifier (UUID string).
using JobId = std::string;

/// Opaque node identifier (hostname or UUID string).
using NodeId = std::string;

/// Bandwidth in bits per second.
using BandwidthBps = uint64_t;

/// Duration alias using nanoseconds internally; convertible to any chrono unit.
using Duration = std::chrono::nanoseconds;

/// Steady-clock time point used for all internal timing.
using TimePoint = std::chrono::steady_clock::time_point;

/// Wall-clock time point used for external events (checkpoints, reclaims).
using WallClock = std::chrono::system_clock::time_point;

// ── Five-tuple ────────────────────────────────────────────────────────────────

/// Five-tuple network flow identifier (IPv4 only).
struct FiveTuple {
  uint32_t src_ip   = 0;
  uint32_t dst_ip   = 0;
  uint16_t src_port = 0;
  uint16_t dst_port = 0;
  uint8_t  protocol = 0;  ///< IPPROTO_TCP=6, IPPROTO_UDP=17

  bool operator==(const FiveTuple& o) const noexcept {
    return src_ip == o.src_ip && dst_ip == o.dst_ip &&
           src_port == o.src_port && dst_port == o.dst_port &&
           protocol == o.protocol;
  }
  bool operator!=(const FiveTuple& o) const noexcept { return !(*this == o); }
};

// ── Enumerations ──────────────────────────────────────────────────────────────

/// State of a node in the cluster registry.
enum class NodeState : uint8_t {
  UNKNOWN   = 0,
  ONLINE    = 1,  ///< Reachable, heartbeat current.
  DEGRADED  = 2,  ///< Missed >= 1 heartbeat.
  OFFLINE   = 3,  ///< Declared dead; no further assignments.
  DRAINING  = 4,  ///< Reclaim in progress, no new jobs.
};

/// Lifecycle state of a GPU job.
enum class JobState : uint8_t {
  UNKNOWN      = 0,
  RUNNING      = 1,
  CHECKPOINTING= 2,
  MIGRATING    = 3,
  SUSPENDED    = 4,
  COMPLETED    = 5,
  FAILED       = 6,
};

/// Reclaim event type distinguishing scheduled vs. emergency eviction.
enum class ReclaimType : uint8_t {
  SCHEDULED  = 0,  ///< Owner returned with tau > 0.
  EMERGENCY  = 1,  ///< Owner returned with tau = 0 (hard deadline).
};

/// Campus topology level determines composite cost gamma component (§VI).
enum class TopologyLevel : uint8_t {
  INTRA_RACK     = 0,  ///< Same rack; negligible transfer overhead.
  INTRA_BUILDING = 1,  ///< Same building; one hop through ToR.
  CROSS_BUILDING = 2,  ///< Different buildings; two aggregation hops.
  CORE_CROSSING  = 3,  ///< Crossing core switch; highest latency tier.
};

/// Phase of an active migration operation.
enum class MigrationPhase : uint8_t {
  IDLE         = 0,
  ADMITTED     = 1,  ///< Admission control passed; rate tokens allocated.
  TRANSFERRING = 2,  ///< Checkpoint bytes in flight.
  RESTORING    = 3,  ///< Destination is restoring GPU state.
  COMPLETED    = 4,
  ABORTED      = 5,
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default migration share of link capacity (P3 §VII).
constexpr double kDefaultMigrationShare = 0.40;

/// Survival horizon used in P2 composite cost calculation (seconds).
constexpr double kSurvivalHorizonSeconds = 3600.0;

/// Token-bucket burst factor: burst = rate * kBurstFactor (seconds of burst).
constexpr double kBurstFactor = 0.050;  ///< 50 ms burst window.

/// Minimum checkpoint interval floor (1 second).
constexpr Duration kMinCheckpointInterval = std::chrono::seconds(1);

/// Maximum checkpoint interval ceiling (10 minutes).
constexpr Duration kMaxCheckpointInterval = std::chrono::minutes(10);

/// Protocol version string.
constexpr const char* kProtocolVersion = "1.0.0";

}  // namespace reclaimnet

// ── std::hash specialisation for FiveTuple ───────────────────────────────────

namespace std {
template <>
struct hash<reclaimnet::FiveTuple> {
  size_t operator()(const reclaimnet::FiveTuple& ft) const noexcept {
    size_t h = 0;
    auto mix = [&](size_t v) { h ^= v + 0x9e3779b9 + (h << 6) + (h >> 2); };
    mix(std::hash<uint32_t>{}(ft.src_ip));
    mix(std::hash<uint32_t>{}(ft.dst_ip));
    mix(std::hash<uint32_t>{}((uint32_t(ft.src_port) << 16) | ft.dst_port));
    mix(std::hash<uint8_t>{}(ft.protocol));
    return h;
  }
};
}  // namespace std
