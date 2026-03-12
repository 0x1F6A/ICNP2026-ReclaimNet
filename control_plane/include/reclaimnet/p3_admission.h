#pragma once
/// @file p3_admission.h
/// @brief P3 – Migration Traffic Control and Admission.
///
/// Maintains per-link token buckets.  Decides whether a new migration
/// can be admitted given the reclaim deadline and remaining link capacity.
/// Signals the eBPF kill-switch when emergency reclaim is required.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <string>

namespace reclaimnet {
namespace control {

/// One active migration tracked by P3.
struct MigrationEntry {
  JobId    job_id;
  NodeId   src;
  NodeId   dst;
  uint64_t data_remaining_bytes;
  double   deadline_s;          ///< absolute POSIX seconds
  double   allocated_bps;
  int64_t  start_ns;
  bool     emergency;
};

/// Per-link admission state.
struct LinkState {
  NodeId   src;
  NodeId   dst;
  double   capacity_bps;
  double   allocated_bps;       ///< sum of active migrations
  size_t   active_count;
};

/// Thread-safe P3 admission controller.
class P3Admission {
public:
  /// @param migration_share  Fraction of link capacity reserved for migrations.
  explicit P3Admission(double migration_share = 0.40);

  /// Register a link with its capacity.
  void add_link(const NodeId& src, const NodeId& dst, double cap_bps);

  /// Try to admit a migration.
  /// Grants bandwidth proportional to (1 - allocated/capacity) if feasible.
  VoidResult admit(const MigrationEntry& entry);

  /// Release resources held by a completed or failed migration.
  void release(const JobId& job_id);

  /// Update progress (reduces data_remaining_bytes).
  void update_progress(const JobId& job_id, uint64_t bytes_done);

  /// Check for overdue migrations (deadline exceeded) and return them.
  std::vector<MigrationEntry> expired_migrations(double now_s) const;

  /// Full snapshot of active migrations.
  std::vector<MigrationEntry> active_migrations() const;

  /// Per-link utilization snapshot.
  std::vector<LinkState> link_states() const;

  /// Emergency drain: marks all migrations on src as emergency.
  void emergency_drain(const NodeId& src);

  size_t active_count() const;

private:
  double migration_share_;
  mutable std::mutex mu_;

  std::unordered_map<JobId, MigrationEntry> migrations_;
  // link key: "src:dst"
  std::unordered_map<std::string, LinkState> links_;

  static std::string link_key(const NodeId& s, const NodeId& d);
  double available_bps(const std::string& key) const;  // caller holds mu_
};

}  // namespace control
}  // namespace reclaimnet
