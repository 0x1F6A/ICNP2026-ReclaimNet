/// @file p3_admission.cc
/// @brief P3 migration admission controller with per-link token buckets.

#include "reclaimnet/p3_admission.h"
#include "reclaimnet/log.h"

#include <algorithm>
#include <sstream>

namespace reclaimnet {
namespace control {

// ── Helpers ───────────────────────────────────────────────────────────────────

std::string P3Admission::link_key(const NodeId& s, const NodeId& d) {
  return s + ":" + d;
}

double P3Admission::available_bps(const std::string& key) const {
  auto it = links_.find(key);
  if (it == links_.end()) return 0.0;
  double cap = it->second.capacity_bps * migration_share_;
  return std::max(0.0, cap - it->second.allocated_bps);
}

// ── Constructor ───────────────────────────────────────────────────────────────

P3Admission::P3Admission(double migration_share)
  : migration_share_(migration_share) {}

// ── add_link ─────────────────────────────────────────────────────────────────

void P3Admission::add_link(const NodeId& src, const NodeId& dst, double cap_bps) {
  std::lock_guard<std::mutex> lk(mu_);
  auto key = link_key(src, dst);
  links_.emplace(key, LinkState{src, dst, cap_bps, 0.0, 0});
}

// ── admit ─────────────────────────────────────────────────────────────────────

VoidResult P3Admission::admit(const MigrationEntry& entry) {
  std::lock_guard<std::mutex> lk(mu_);

  auto key = link_key(entry.src, entry.dst);
  double avail = available_bps(key);

  if (avail <= 0.0) {
    return err_void("No bandwidth available on link " + key +
                    " for job " + entry.job_id);
  }

  // Grant all available bandwidth (P3 greedy admit).
  MigrationEntry e       = entry;
  e.allocated_bps        = avail;
  migrations_.emplace(entry.job_id, e);

  auto& lk_state         = links_[key];
  lk_state.allocated_bps += avail;
  lk_state.active_count++;

  LOG_INFO("P3: admitted job=%s on %s→%s, %.0f bps, deadline=%.1fs",
           entry.job_id.c_str(), entry.src.c_str(), entry.dst.c_str(),
           avail, entry.deadline_s);
  return ok_void();
}

// ── release ───────────────────────────────────────────────────────────────────

void P3Admission::release(const JobId& job_id) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = migrations_.find(job_id);
  if (it == migrations_.end()) return;

  const MigrationEntry& e = it->second;
  auto key = link_key(e.src, e.dst);
  auto& ls = links_[key];
  ls.allocated_bps = std::max(0.0, ls.allocated_bps - e.allocated_bps);
  if (ls.active_count > 0) ls.active_count--;

  migrations_.erase(it);
  LOG_DEBUG("P3: released job=%s", job_id.c_str());
}

// ── update_progress ───────────────────────────────────────────────────────────

void P3Admission::update_progress(const JobId& job_id, uint64_t bytes_done) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = migrations_.find(job_id);
  if (it == migrations_.end()) return;
  if (bytes_done >= it->second.data_remaining_bytes)
    it->second.data_remaining_bytes = 0;
  else
    it->second.data_remaining_bytes -= bytes_done;
}

// ── expired_migrations ────────────────────────────────────────────────────────

std::vector<MigrationEntry> P3Admission::expired_migrations(double now_s) const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<MigrationEntry> out;
  for (auto& [_, e] : migrations_)
    if (e.deadline_s < now_s) out.push_back(e);
  return out;
}

// ── active_migrations ─────────────────────────────────────────────────────────

std::vector<MigrationEntry> P3Admission::active_migrations() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<MigrationEntry> out;
  out.reserve(migrations_.size());
  for (auto& [_, e] : migrations_) out.push_back(e);
  return out;
}

// ── link_states ───────────────────────────────────────────────────────────────

std::vector<LinkState> P3Admission::link_states() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<LinkState> out;
  out.reserve(links_.size());
  for (auto& [_, ls] : links_) out.push_back(ls);
  return out;
}

// ── emergency_drain ───────────────────────────────────────────────────────────

void P3Admission::emergency_drain(const NodeId& src) {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& [_, e] : migrations_)
    if (e.src == src) e.emergency = true;
  LOG_WARN("P3: emergency drain signalled for node %s", src.c_str());
}

// ── active_count ─────────────────────────────────────────────────────────────

size_t P3Admission::active_count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return migrations_.size();
}

}  // namespace control
}  // namespace reclaimnet
