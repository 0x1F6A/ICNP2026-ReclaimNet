#pragma once
/// @file persistence.h
/// @brief SQLite-backed persistence for job and node records.
///
/// Provides crash-recovery: on coordinator restart the in-memory registries
/// can be repopulated from SQLite.

#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/job_registry.h"
#include "reclaimnet/node_registry.h"

#include <string>
#include <vector>
#include <memory>

// Forward-declare sqlite3 to avoid including sqlite3.h in this header.
struct sqlite3;

namespace reclaimnet {
namespace control {

/// Thin RAII wrapper around a SQLite database connection.
class PersistenceStore {
public:
  explicit PersistenceStore(const std::string& db_path);
  ~PersistenceStore();

  PersistenceStore(const PersistenceStore&) = delete;
  PersistenceStore& operator=(const PersistenceStore&) = delete;

  /// Create schema tables if they do not already exist.
  VoidResult init_schema();

  // ── Job persistence ───────────────────────────────────────────────────────

  VoidResult upsert_job(const JobRecord& rec);
  VoidResult delete_job(const JobId& id);
  Result<std::vector<JobRecord>> load_all_jobs();

  // ── Node persistence ──────────────────────────────────────────────────────

  VoidResult upsert_node(const NodeRecord& rec);
  VoidResult delete_node(const NodeId& id);
  Result<std::vector<NodeRecord>> load_all_nodes();

  // ── Event log ─────────────────────────────────────────────────────────────

  /// Append a generic event string for audit / replay.
  VoidResult log_event(const std::string& type, const std::string& payload_json);

  /// Retrieve recent events.
  Result<std::vector<std::string>> recent_events(int limit = 100);

  bool is_open() const noexcept { return db_ != nullptr; }

private:
  sqlite3* db_ = nullptr;

  VoidResult exec(const char* sql);
};

}  // namespace control
}  // namespace reclaimnet
