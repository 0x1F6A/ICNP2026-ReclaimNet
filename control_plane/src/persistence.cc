#include "reclaimnet/persistence.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"
#include <filesystem>
#include <functional>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
#if RECLAIMNET_HAVE_SQLITE
#include <sqlite3.h>
#endif

namespace reclaimnet {
namespace control {
#if RECLAIMNET_HAVE_SQLITE
namespace {
struct Statement {
  sqlite3_stmt *ptr = nullptr;
  Statement(sqlite3 *db, const char *sql) {
    sqlite3_prepare_v2(db, sql, -1, &ptr, nullptr);
  }
  ~Statement() {
    if (ptr)
      sqlite3_finalize(ptr);
  }
  Statement(const Statement &) = delete;
  Statement &operator=(const Statement &) = delete;
};
VoidResult execute(sqlite3 *db, const char *sql) {
  if (!db)
    return err_void("Database is not open");
  char *message = nullptr;
  int rc = sqlite3_exec(db, sql, nullptr, nullptr, &message);
  std::string error = message ? message : "";
  sqlite3_free(message);
  if (rc != SQLITE_OK)
    return err_void("SQLite: " + error);
  return ok_void();
}
VoidResult step(sqlite3 *db, Statement &stmt) {
  if (!stmt.ptr || sqlite3_step(stmt.ptr) != SQLITE_DONE)
    return err_void(std::string("SQLite statement: ") + sqlite3_errmsg(db));
  return ok_void();
}
VoidResult transaction(sqlite3 *db, const std::function<VoidResult()> &body) {
  if (auto begin = execute(db, "BEGIN IMMEDIATE;"); !begin)
    return begin;
  auto result = body();
  if (!result) {
    execute(db, "ROLLBACK;");
    return result;
  }
  auto commit = execute(db, "COMMIT;");
  if (!commit)
    execute(db, "ROLLBACK;");
  return commit;
}
void bind_text(Statement &stmt, int index, const std::string &value) {
  sqlite3_bind_text(stmt.ptr, index, value.c_str(), -1, SQLITE_TRANSIENT);
}
std::string text(sqlite3_stmt *stmt, int col) {
  auto value = reinterpret_cast<const char *>(sqlite3_column_text(stmt, col));
  return value ? value : "";
}
}
#endif

PersistenceStore::PersistenceStore(const std::string &path) {
#if RECLAIMNET_HAVE_SQLITE
  if (path != ":memory:") {
    std::error_code error;
    auto parent = std::filesystem::path(path).parent_path();
    if (!parent.empty())
      std::filesystem::create_directories(parent, error);
    if (error) {
      LOG_ERR("Cannot create DB directory: %s", error.message().c_str());
      return;
    }
  }
  if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
    LOG_ERR("Cannot open SQLite database: %s", sqlite3_errmsg(db_));
    sqlite3_close(db_);
    db_ = nullptr;
  } else {
    sqlite3_busy_timeout(db_, 5000);
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");
  }
#else
  (void)path;
#endif
}
PersistenceStore::~PersistenceStore() {
#if RECLAIMNET_HAVE_SQLITE
  if (db_)
    sqlite3_close(db_);
#endif
}
VoidResult PersistenceStore::exec(const char *sql) {
#if RECLAIMNET_HAVE_SQLITE
  return execute(db_, sql);
#else
  (void)sql;
  return ok_void();
#endif
}
VoidResult PersistenceStore::init_schema() {

  return exec(R"SQL(
    CREATE TABLE IF NOT EXISTS jobs (
      job_id TEXT PRIMARY KEY, node_id TEXT, state INTEGER, submit_time_ns INTEGER,
      last_update_ns INTEGER, vram_bytes INTEGER, checkpoint_size INTEGER,
      lambda_i REAL, progress REAL, user_name TEXT, workload_tag TEXT);
    CREATE TABLE IF NOT EXISTS nodes (
      node_id TEXT PRIMARY KEY, state INTEGER, address TEXT, building TEXT,
      switch_id TEXT, total_vram INTEGER, link_cap_bps INTEGER, registered_at INTEGER);
    CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, payload TEXT, ts_ns INTEGER);
    CREATE TABLE IF NOT EXISTS job_runtime (
      job_id TEXT PRIMARY KEY, migration_dst TEXT, checkpoint_interval_s REAL,
      checkpoint_allocated_bps REAL, checkpoint_write_rate_bps REAL,
      write_cap_bps REAL, loss_budget_s REAL, checkpoint_constraints_feasible INTEGER);
    CREATE TABLE IF NOT EXISTS node_runtime (
      node_id TEXT PRIMARY KEY, last_heartbeat_ns INTEGER, gpu_util_pct REAL,
      vram_used_pct REAL, vram_free_bytes INTEGER, link_tx_bps REAL,
      link_rx_bps REAL, cpu_load_1m REAL, updated_ns INTEGER);
    CREATE TABLE IF NOT EXISTS extended_runtime (
      kind TEXT, identity TEXT, payload TEXT, PRIMARY KEY(kind,identity));
  )SQL");
}
VoidResult PersistenceStore::upsert_job(const JobRecord &job) {
#if RECLAIMNET_HAVE_SQLITE
  return transaction(db_, [&]() -> VoidResult {
    Statement stmt(db_, "INSERT OR REPLACE INTO jobs "
                        "(job_id,node_id,state,submit_time_ns,last_update_ns,"
                        "vram_bytes,checkpoint_size,"
                        "lambda_i,progress,user_name,workload_tag) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?);");
    if (!stmt.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(stmt, 1, job.job_id);
    bind_text(stmt, 2, job.node_id);
    sqlite3_bind_int(stmt.ptr, 3, static_cast<int>(job.state));
    sqlite3_bind_int64(stmt.ptr, 4, job.submit_time_ns);
    sqlite3_bind_int64(stmt.ptr, 5, job.last_update_ns);
    sqlite3_bind_int64(stmt.ptr, 6, job.vram_bytes);
    sqlite3_bind_int64(stmt.ptr, 7, job.checkpoint_size);
    sqlite3_bind_double(stmt.ptr, 8, job.lambda_i);
    sqlite3_bind_double(stmt.ptr, 9, job.progress);
    bind_text(stmt, 10, job.user);
    bind_text(stmt, 11, job.workload_tag);
    if (auto result = step(db_, stmt); !result)
      return result;
    Statement runtime(
        db_, "INSERT OR REPLACE INTO job_runtime VALUES(?,?,?,?,?,?,?,?);");
    if (!runtime.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(runtime, 1, job.job_id);
    bind_text(runtime, 2, job.migration_dst);
    sqlite3_bind_double(runtime.ptr, 3, job.checkpoint_interval_s);
    sqlite3_bind_double(runtime.ptr, 4, job.checkpoint_allocated_bps);
    sqlite3_bind_double(runtime.ptr, 5, job.checkpoint_write_rate_bps);
    sqlite3_bind_double(runtime.ptr, 6, job.write_cap_bps);
    sqlite3_bind_double(runtime.ptr, 7, job.loss_budget_s);
    sqlite3_bind_int(runtime.ptr, 8, job.checkpoint_constraints_feasible);
    if (auto result = step(db_, runtime); !result)
      return result;
#if RECLAIMNET_HAVE_JSON
    return set_runtime(
        "job_fields", job.job_id,
        nlohmann::json{
            {"cuda_capability_required", job.cuda_capability_required},
            {"gpu_count_required", job.gpu_count_required},
            {"remaining_runtime_s", job.remaining_runtime_s},
            {"rank_nodes", job.rank_nodes},
            {"rank_count", job.rank_count},
            {"rank_checkpoint_sizes", job.rank_checkpoint_sizes}}
            .dump());
#else
    return ok_void();
#endif
  });
#else
  (void)job;
  return ok_void();
#endif
}
VoidResult PersistenceStore::delete_job(const JobId &id) {
#if RECLAIMNET_HAVE_SQLITE
  return transaction(db_, [&]() -> VoidResult {
    Statement runtime(db_, "DELETE FROM job_runtime WHERE job_id=?;");
    Statement job(db_, "DELETE FROM jobs WHERE job_id=?;");
    if (!runtime.ptr || !job.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(runtime, 1, id);
    bind_text(job, 1, id);
    if (auto result = step(db_, runtime); !result)
      return result;
    return step(db_, job);
  });
#else
  (void)id;
  return ok_void();
#endif
}
Result<std::vector<JobRecord>> PersistenceStore::load_all_jobs() {
  std::vector<JobRecord> jobs;
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return Error{"Database is not open"};
  Statement stmt(
      db_,
      "SELECT j.job_id,j.node_id,j.state,j.submit_time_ns,j.last_update_ns,"
      "j.vram_bytes,j.checkpoint_size,j.lambda_i,j.progress,j.user_name,j."
      "workload_tag,"
      "r.migration_dst,r.checkpoint_interval_s,r.checkpoint_allocated_bps,"
      "r.checkpoint_write_rate_bps,r.write_cap_bps,COALESCE(r.loss_budget_s,0),"
      "r.checkpoint_constraints_feasible FROM jobs j LEFT JOIN job_runtime r "
      "USING(job_id);");
  if (!stmt.ptr)
    return Error{sqlite3_errmsg(db_)};
  int rc;
  while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) {
    JobRecord job{};
    job.job_id = text(stmt.ptr, 0);
    job.node_id = text(stmt.ptr, 1);
    job.state = static_cast<JobState>(sqlite3_column_int(stmt.ptr, 2));
    job.submit_time_ns = sqlite3_column_int64(stmt.ptr, 3);
    job.last_update_ns = sqlite3_column_int64(stmt.ptr, 4);
    job.vram_bytes = sqlite3_column_int64(stmt.ptr, 5);
    job.checkpoint_size = sqlite3_column_int64(stmt.ptr, 6);
    job.lambda_i = sqlite3_column_double(stmt.ptr, 7);
    job.progress = sqlite3_column_double(stmt.ptr, 8);
    job.user = text(stmt.ptr, 9);
    job.workload_tag = text(stmt.ptr, 10);
    job.migration_dst = text(stmt.ptr, 11);
    job.checkpoint_interval_s = sqlite3_column_double(stmt.ptr, 12);
    job.checkpoint_allocated_bps = sqlite3_column_double(stmt.ptr, 13);
    job.checkpoint_write_rate_bps = sqlite3_column_double(stmt.ptr, 14);
    job.write_cap_bps = sqlite3_column_double(stmt.ptr, 15);
    job.loss_budget_s = sqlite3_column_double(stmt.ptr, 16);
    job.checkpoint_constraints_feasible = sqlite3_column_int(stmt.ptr, 17) != 0;
#if RECLAIMNET_HAVE_JSON
    auto extra = get_runtime("job_fields", job.job_id);
    if (!extra)
      return extra.error();
    if (!extra.value().empty()) {
      try {
        const auto fields = nlohmann::json::parse(extra.value());
        job.cuda_capability_required =
            fields.value("cuda_capability_required", 0.0);
        job.gpu_count_required = fields.value("gpu_count_required", 1u);
        job.remaining_runtime_s = fields.value("remaining_runtime_s", 3600.0);
        job.rank_nodes = fields.value("rank_nodes", std::vector<NodeId>{});
        job.rank_count = fields.value("rank_count", 1u);
        job.rank_checkpoint_sizes =
            fields.value("rank_checkpoint_sizes", std::vector<uint64_t>{});
      } catch (const std::exception &error) {
        return Error{error.what()};
      }
    }
#endif
    jobs.push_back(std::move(job));
  }
  if (rc != SQLITE_DONE)
    return Error{sqlite3_errmsg(db_)};
#endif
  return jobs;
}
VoidResult PersistenceStore::upsert_node(const NodeRecord &node) {
#if RECLAIMNET_HAVE_SQLITE
  return transaction(db_, [&]() -> VoidResult {
    Statement stmt(db_, "INSERT OR REPLACE INTO nodes "
                        "(node_id,state,address,building,switch_id,total_vram,"
                        "link_cap_bps,registered_at)"
                        " VALUES(?,?,?,?,?,?,?,?);");
    if (!stmt.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(stmt, 1, node.node_id);
    sqlite3_bind_int(stmt.ptr, 2, static_cast<int>(node.state));
    bind_text(stmt, 3, node.address);
    bind_text(stmt, 4, node.building);
    bind_text(stmt, 5, node.switch_id);
    sqlite3_bind_int64(stmt.ptr, 6, node.total_vram_bytes);
    sqlite3_bind_int64(stmt.ptr, 7, node.link_capacity_bps);
    sqlite3_bind_int64(stmt.ptr, 8, node.registered_at_ns);
    if (auto result = step(db_, stmt); !result)
      return result;
    Statement runtime(
        db_, "INSERT OR REPLACE INTO node_runtime VALUES(?,?,?,?,?,?,?,?,?);");
    if (!runtime.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(runtime, 1, node.node_id);
    sqlite3_bind_int64(runtime.ptr, 2, node.last_heartbeat_ns);
    sqlite3_bind_double(runtime.ptr, 3, node.telemetry.gpu_util_pct);
    sqlite3_bind_double(runtime.ptr, 4, node.telemetry.vram_used_pct);
    sqlite3_bind_int64(runtime.ptr, 5, node.telemetry.vram_free_bytes);
    sqlite3_bind_double(runtime.ptr, 6, node.telemetry.link_tx_bps);
    sqlite3_bind_double(runtime.ptr, 7, node.telemetry.link_rx_bps);
    sqlite3_bind_double(runtime.ptr, 8, node.telemetry.cpu_load_1m);
    sqlite3_bind_int64(runtime.ptr, 9, node.telemetry.updated_ns);
    if (auto result = step(db_, runtime); !result)
      return result;
#if RECLAIMNET_HAVE_JSON
    return set_runtime(
        "node_fields", node.node_id,
        nlohmann::json{
            {"gpu_count", node.gpu_count},
            {"gpu_free_count", node.gpu_free_count},
            {"cuda_capability", node.cuda_capability},
            {"historical_migration_cost_s", node.historical_migration_cost_s},
            {"historical_migration_cost_updated_ns",
             node.historical_migration_cost_updated_ns}}
            .dump());
#else
    return ok_void();
#endif
  });
#else
  (void)node;
  return ok_void();
#endif
}
VoidResult PersistenceStore::delete_node(const NodeId &id) {
#if RECLAIMNET_HAVE_SQLITE
  return transaction(db_, [&]() -> VoidResult {
    Statement runtime(db_, "DELETE FROM node_runtime WHERE node_id=?;");
    Statement node(db_, "DELETE FROM nodes WHERE node_id=?;");
    if (!runtime.ptr || !node.ptr)
      return err_void(sqlite3_errmsg(db_));
    bind_text(runtime, 1, id);
    bind_text(node, 1, id);
    if (auto result = step(db_, runtime); !result)
      return result;
    return step(db_, node);
  });
#else
  (void)id;
  return ok_void();
#endif
}
Result<std::vector<NodeRecord>> PersistenceStore::load_all_nodes() {
  std::vector<NodeRecord> nodes;
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return Error{"Database is not open"};
  Statement stmt(
      db_,
      "SELECT n.node_id,n.state,n.address,n.building,n.switch_id,n.total_vram,"
      "n.link_cap_bps,n.registered_at,r.last_heartbeat_ns,r.gpu_util_pct,r."
      "vram_used_pct,"
      "r.vram_free_bytes,r.link_tx_bps,r.link_rx_bps,r.cpu_load_1m,r.updated_"
      "ns "
      "FROM nodes n LEFT JOIN node_runtime r USING(node_id);");
  if (!stmt.ptr)
    return Error{sqlite3_errmsg(db_)};
  int rc;
  while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) {
    NodeRecord node{};
    node.node_id = text(stmt.ptr, 0);
    node.state = static_cast<NodeState>(sqlite3_column_int(stmt.ptr, 1));
    node.address = text(stmt.ptr, 2);
    node.building = text(stmt.ptr, 3);
    node.switch_id = text(stmt.ptr, 4);
    node.total_vram_bytes = sqlite3_column_int64(stmt.ptr, 5);
    node.link_capacity_bps = sqlite3_column_int64(stmt.ptr, 6);
    node.registered_at_ns = sqlite3_column_int64(stmt.ptr, 7);
    node.last_heartbeat_ns = sqlite3_column_int64(stmt.ptr, 8);
    node.telemetry.gpu_util_pct = sqlite3_column_double(stmt.ptr, 9);
    node.telemetry.vram_used_pct = sqlite3_column_double(stmt.ptr, 10);
    node.telemetry.vram_free_bytes = sqlite3_column_int64(stmt.ptr, 11);
    node.telemetry.link_tx_bps = sqlite3_column_double(stmt.ptr, 12);
    node.telemetry.link_rx_bps = sqlite3_column_double(stmt.ptr, 13);
    node.telemetry.cpu_load_1m = sqlite3_column_double(stmt.ptr, 14);
    node.telemetry.updated_ns = sqlite3_column_int64(stmt.ptr, 15);
#if RECLAIMNET_HAVE_JSON
    auto extra = get_runtime("node_fields", node.node_id);
    if (!extra)
      return extra.error();
    if (!extra.value().empty()) {
      try {
        const auto fields = nlohmann::json::parse(extra.value());
        node.gpu_count = fields.value("gpu_count", 1u);
        node.gpu_free_count = fields.value("gpu_free_count", 1u);
        node.cuda_capability = fields.value("cuda_capability", 0.0);
        node.telemetry.gpu_free_count = node.gpu_free_count;
        node.telemetry.cuda_capability = node.cuda_capability;
        node.historical_migration_cost_s =
            fields.value("historical_migration_cost_s", 0.0);
        node.historical_migration_cost_updated_ns =
            fields.value("historical_migration_cost_updated_ns", int64_t{0});
      } catch (const std::exception &error) {
        return Error{error.what()};
      }
    }
#endif
    nodes.push_back(std::move(node));
  }
  if (rc != SQLITE_DONE)
    return Error{sqlite3_errmsg(db_)};
#endif
  return nodes;
}
VoidResult PersistenceStore::log_event(const std::string &type,
                                       const std::string &payload,
                                       int64_t timestamp_ns) {
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return err_void("Database is not open");
  Statement stmt(db_, "INSERT INTO events(type,payload,ts_ns) VALUES(?,?,?);");
  if (!stmt.ptr)
    return err_void(sqlite3_errmsg(db_));
  bind_text(stmt, 1, type);
  bind_text(stmt, 2, payload);
  sqlite3_bind_int64(stmt.ptr, 3, timestamp_ns > 0 ? timestamp_ns : now_ns());
  return step(db_, stmt);
#else
  (void)type;
  (void)payload;
  (void)timestamp_ns;
  return ok_void();
#endif
}
Result<std::vector<std::string>> PersistenceStore::recent_events(int limit) {
  std::vector<std::string> events;
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return Error{"Database is not open"};
  if (limit < 0)
    return Error{"Event limit must be nonnegative"};
  Statement stmt(
      db_, "SELECT type||'|'||payload FROM events ORDER BY id DESC LIMIT ?;");
  if (!stmt.ptr)
    return Error{sqlite3_errmsg(db_)};
  sqlite3_bind_int(stmt.ptr, 1, limit);
  int rc;
  while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW)
    events.push_back(text(stmt.ptr, 0));
  if (rc != SQLITE_DONE)
    return Error{sqlite3_errmsg(db_)};
#else
  (void)limit;
#endif
  return events;
}
VoidResult PersistenceStore::set_runtime(const std::string &kind,
                                         const std::string &id,
                                         const std::string &payload) {
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return err_void("Database is not open");
  Statement statement(db_,
                      "INSERT OR REPLACE INTO "
                      "extended_runtime(kind,identity,payload) VALUES(?,?,?);");
  if (!statement.ptr)
    return err_void(sqlite3_errmsg(db_));
  bind_text(statement, 1, kind);
  bind_text(statement, 2, id);
  bind_text(statement, 3, payload);
  return step(db_, statement);
#else
  (void)kind;
  (void)id;
  (void)payload;
  return ok_void();
#endif
}
Result<std::string> PersistenceStore::get_runtime(const std::string &kind,
                                                  const std::string &id) {
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return Error{"Database is not open"};
  Statement statement(
      db_, "SELECT payload FROM extended_runtime WHERE kind=? AND identity=?;");
  if (!statement.ptr)
    return Error{sqlite3_errmsg(db_)};
  bind_text(statement, 1, kind);
  bind_text(statement, 2, id);
  const int status = sqlite3_step(statement.ptr);
  if (status == SQLITE_ROW)
    return text(statement.ptr, 0);
  if (status == SQLITE_DONE)
    return std::string{};
  return Error{sqlite3_errmsg(db_)};
#else
  (void)kind;
  (void)id;
  return std::string{};
#endif
}
Result<std::vector<std::pair<std::string, std::string>>>
PersistenceStore::load_runtime(const std::string &kind) {
  std::vector<std::pair<std::string, std::string>> result;
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return Error{"Database is not open"};
  Statement statement(
      db_, "SELECT identity,payload FROM extended_runtime WHERE kind=?;");
  if (!statement.ptr)
    return Error{sqlite3_errmsg(db_)};
  bind_text(statement, 1, kind);
  int status;
  while ((status = sqlite3_step(statement.ptr)) == SQLITE_ROW)
    result.emplace_back(text(statement.ptr, 0), text(statement.ptr, 1));
  if (status != SQLITE_DONE)
    return Error{sqlite3_errmsg(db_)};
#else
  (void)kind;
#endif
  return result;
}
VoidResult PersistenceStore::erase_runtime(const std::string &kind,
                                           const std::string &id) {
#if RECLAIMNET_HAVE_SQLITE
  if (!db_)
    return err_void("Database is not open");
  Statement statement(
      db_, "DELETE FROM extended_runtime WHERE kind=? AND identity=?;");
  if (!statement.ptr)
    return err_void(sqlite3_errmsg(db_));
  bind_text(statement, 1, kind);
  bind_text(statement, 2, id);
  return step(db_, statement);
#else
  (void)kind;
  (void)id;
  return ok_void();
#endif
}

}
}
