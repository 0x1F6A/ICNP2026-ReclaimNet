#pragma once

#include "reclaimnet/job_registry.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <memory>
#include <string>
#include <vector>

struct sqlite3;

namespace reclaimnet {
namespace control {

class PersistenceStore {
public:
  explicit PersistenceStore(const std::string &db_path);
  ~PersistenceStore();

  PersistenceStore(const PersistenceStore &) = delete;
  PersistenceStore &operator=(const PersistenceStore &) = delete;

  VoidResult init_schema();

  VoidResult upsert_job(const JobRecord &rec);
  VoidResult delete_job(const JobId &id);
  Result<std::vector<JobRecord>> load_all_jobs();

  VoidResult upsert_node(const NodeRecord &rec);
  VoidResult delete_node(const NodeId &id);
  Result<std::vector<NodeRecord>> load_all_nodes();

  VoidResult log_event(const std::string &type, const std::string &payload_json,
                       int64_t timestamp_ns = 0);

  Result<std::vector<std::string>> recent_events(int limit = 100);

  bool is_open() const noexcept { return db_ != nullptr; }
  VoidResult set_runtime(const std::string &kind, const std::string &id,
                         const std::string &payload);
  Result<std::string> get_runtime(const std::string &kind,
                                  const std::string &id);
  Result<std::vector<std::pair<std::string, std::string>>>
  load_runtime(const std::string &kind);
  VoidResult erase_runtime(const std::string &kind, const std::string &id);

private:
  sqlite3 *db_ = nullptr;

  VoidResult exec(const char *sql);
};

}
}
