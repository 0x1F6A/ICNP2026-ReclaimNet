


#include "reclaimnet/persistence.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#if __has_include(<sqlite3.h>)
#  include <sqlite3.h>
#  define HAVE_SQLITE 1
#else
#  define HAVE_SQLITE 0

   typedef struct sqlite3 sqlite3;
#endif

#include <sstream>
#include <cstring>

namespace reclaimnet {
namespace control {



PersistenceStore::PersistenceStore(const std::string& db_path) {
#if HAVE_SQLITE
  int rc = sqlite3_open(db_path.c_str(), &db_);
  if (rc != SQLITE_OK) {
    LOG_ERR("SQLite: cannot open %s: %s", db_path.c_str(), sqlite3_errmsg(db_));
    sqlite3_close(db_);
    db_ = nullptr;
  } else {
    LOG_INFO("SQLite: opened %s", db_path.c_str());

    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");
  }
#else
  LOG_WARN("SQLite not available; persistence disabled (path=%s)", db_path.c_str());
#endif
}

PersistenceStore::~PersistenceStore() {
#if HAVE_SQLITE
  if (db_) sqlite3_close(db_);
#endif
}



VoidResult PersistenceStore::exec(const char* sql) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  char* errmsg = nullptr;
  int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &errmsg);
  if (rc != SQLITE_OK) {
    std::string msg = errmsg ? errmsg : "unknown error";
    sqlite3_free(errmsg);
    return Error{"SQLite exec error: " + msg};
  }
  return ok_void();
#else
  (void)sql;
  return ok_void();
#endif
}



VoidResult PersistenceStore::init_schema() {
  static const char* schema = R"SQL(
    CREATE TABLE IF NOT EXISTS jobs (
      job_id          TEXT PRIMARY KEY,
      node_id         TEXT,
      state           INTEGER,
      submit_time_ns  INTEGER,
      last_update_ns  INTEGER,
      vram_bytes      INTEGER,
      checkpoint_size INTEGER,
      lambda_i        REAL,
      progress        REAL,
      user_name       TEXT,
      workload_tag    TEXT
    );
    CREATE TABLE IF NOT EXISTS nodes (
      node_id         TEXT PRIMARY KEY,
      state           INTEGER,
      address         TEXT,
      building        TEXT,
      switch_id       TEXT,
      total_vram      INTEGER,
      link_cap_bps    INTEGER,
      registered_at   INTEGER
    );
    CREATE TABLE IF NOT EXISTS events (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      type     TEXT,
      payload  TEXT,
      ts_ns    INTEGER
    );
  )SQL";
  return exec(schema);
}



VoidResult PersistenceStore::upsert_job(const JobRecord& rec) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  static const char* sql =
    "INSERT OR REPLACE INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,?);";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  sqlite3_bind_text (stmt, 1, rec.job_id.c_str(),       -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (stmt, 2, rec.node_id.c_str(),      -1, SQLITE_TRANSIENT);
  sqlite3_bind_int  (stmt, 3, (int)rec.state);
  sqlite3_bind_int64(stmt, 4, (int64_t)rec.submit_time_ns);
  sqlite3_bind_int64(stmt, 5, (int64_t)rec.last_update_ns);
  sqlite3_bind_int64(stmt, 6, (int64_t)rec.vram_bytes);
  sqlite3_bind_int64(stmt, 7, (int64_t)rec.checkpoint_size);
  sqlite3_bind_double(stmt, 8, rec.lambda_i);
  sqlite3_bind_double(stmt, 9, rec.progress);
  sqlite3_bind_text (stmt, 10, rec.user.c_str(),         -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (stmt, 11, rec.workload_tag.c_str(), -1, SQLITE_TRANSIENT);
  int rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  if (rc != SQLITE_DONE) return Error{"SQLite upsert_job failed"};
  return ok_void();
#else
  (void)rec; return ok_void();
#endif
}

VoidResult PersistenceStore::delete_job(const JobId& id) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  std::string sql = "DELETE FROM jobs WHERE job_id='" + id + "';";
  return exec(sql.c_str());
#else
  (void)id; return ok_void();
#endif
}

Result<std::vector<JobRecord>> PersistenceStore::load_all_jobs() {
  std::vector<JobRecord> out;
#if HAVE_SQLITE
  if (!db_) return out;
  static const char* sql = "SELECT * FROM jobs;";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    JobRecord r{};
    auto text = [&](int col) -> std::string {
      const char* s = (const char*)sqlite3_column_text(stmt, col);
      return s ? s : "";
    };
    r.job_id           = text(0);
    r.node_id          = text(1);
    r.state            = (JobState)sqlite3_column_int(stmt, 2);
    r.submit_time_ns   = sqlite3_column_int64(stmt, 3);
    r.last_update_ns   = sqlite3_column_int64(stmt, 4);
    r.vram_bytes       = (uint64_t)sqlite3_column_int64(stmt, 5);
    r.checkpoint_size  = (uint64_t)sqlite3_column_int64(stmt, 6);
    r.lambda_i         = sqlite3_column_double(stmt, 7);
    r.progress         = sqlite3_column_double(stmt, 8);
    r.user             = text(9);
    r.workload_tag     = text(10);
    out.push_back(r);
  }
  sqlite3_finalize(stmt);
#endif
  return out;
}



VoidResult PersistenceStore::upsert_node(const NodeRecord& rec) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  static const char* sql =
    "INSERT OR REPLACE INTO nodes VALUES(?,?,?,?,?,?,?,?);";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  sqlite3_bind_text (stmt, 1, rec.node_id.c_str(),  -1, SQLITE_TRANSIENT);
  sqlite3_bind_int  (stmt, 2, (int)rec.state);
  sqlite3_bind_text (stmt, 3, rec.address.c_str(),  -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (stmt, 4, rec.building.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (stmt, 5, rec.switch_id.c_str(),-1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 6, (int64_t)rec.total_vram_bytes);
  sqlite3_bind_int64(stmt, 7, (int64_t)rec.link_capacity_bps);
  sqlite3_bind_int64(stmt, 8, rec.registered_at_ns);
  int rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  if (rc != SQLITE_DONE) return Error{"SQLite upsert_node failed"};
  return ok_void();
#else
  (void)rec; return ok_void();
#endif
}

VoidResult PersistenceStore::delete_node(const NodeId& id) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  std::string sql = "DELETE FROM nodes WHERE node_id='" + id + "';";
  return exec(sql.c_str());
#else
  (void)id; return ok_void();
#endif
}

Result<std::vector<NodeRecord>> PersistenceStore::load_all_nodes() {
  std::vector<NodeRecord> out;
#if HAVE_SQLITE
  if (!db_) return out;
  static const char* sql = "SELECT * FROM nodes;";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    NodeRecord r{};
    auto text = [&](int col) -> std::string {
      const char* s = (const char*)sqlite3_column_text(stmt, col);
      return s ? s : "";
    };
    r.node_id            = text(0);
    r.state              = (NodeState)sqlite3_column_int(stmt, 1);
    r.address            = text(2);
    r.building           = text(3);
    r.switch_id          = text(4);
    r.total_vram_bytes   = (uint64_t)sqlite3_column_int64(stmt, 5);
    r.link_capacity_bps  = (BandwidthBps)sqlite3_column_int64(stmt, 6);
    r.registered_at_ns   = sqlite3_column_int64(stmt, 7);
    out.push_back(r);
  }
  sqlite3_finalize(stmt);
#endif
  return out;
}



VoidResult PersistenceStore::log_event(const std::string& type,
                                        const std::string& payload_json) {
#if HAVE_SQLITE
  if (!db_) return err_void("Database not open");
  static const char* sql = "INSERT INTO events(type,payload,ts_ns) VALUES(?,?,?);";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  sqlite3_bind_text (stmt, 1, type.c_str(),         -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (stmt, 2, payload_json.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 3, now_ns());
  int rc = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  if (rc != SQLITE_DONE) return Error{"SQLite log_event failed"};
  return ok_void();
#else
  (void)type; (void)payload_json; return ok_void();
#endif
}

Result<std::vector<std::string>> PersistenceStore::recent_events(int limit) {
  std::vector<std::string> out;
#if HAVE_SQLITE
  if (!db_) return out;
  std::string sql = "SELECT type||'|'||payload FROM events ORDER BY id DESC LIMIT "
                    + std::to_string(limit) + ";";
  sqlite3_stmt* stmt = nullptr;
  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char* s = (const char*)sqlite3_column_text(stmt, 0);
    if (s) out.push_back(s);
  }
  sqlite3_finalize(stmt);
#endif
  return out;
}

}
}
