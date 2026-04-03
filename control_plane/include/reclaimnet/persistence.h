#pragma once






#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/job_registry.h"
#include "reclaimnet/node_registry.h"

#include <string>
#include <vector>
#include <memory>


struct sqlite3;

namespace reclaimnet {
namespace control {


class PersistenceStore {
public:
  explicit PersistenceStore(const std::string& db_path);
  ~PersistenceStore();

  PersistenceStore(const PersistenceStore&) = delete;
  PersistenceStore& operator=(const PersistenceStore&) = delete;


  VoidResult init_schema();



  VoidResult upsert_job(const JobRecord& rec);
  VoidResult delete_job(const JobId& id);
  Result<std::vector<JobRecord>> load_all_jobs();



  VoidResult upsert_node(const NodeRecord& rec);
  VoidResult delete_node(const NodeId& id);
  Result<std::vector<NodeRecord>> load_all_nodes();




  VoidResult log_event(const std::string& type, const std::string& payload_json);


  Result<std::vector<std::string>> recent_events(int limit = 100);

  bool is_open() const noexcept { return db_ != nullptr; }

private:
  sqlite3* db_ = nullptr;

  VoidResult exec(const char* sql);
};

}
}
