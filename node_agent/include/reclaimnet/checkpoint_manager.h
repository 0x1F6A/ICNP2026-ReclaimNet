#pragma once
#include "reclaimnet/runtime.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
namespace reclaimnet::agent {
struct CheckpointMeta {
  JobId job_id;
  uint32_t sequence = 0;
  uint64_t size_bytes = 0;
  int64_t timestamp_ns = 0;
  std::string path;
  bool complete = false;
  std::string sha256;
  bool restored = false;
  std::string transfer_id;
  uint32_t rank = 0;
};
struct CheckpointCallbacks {
  std::function<void(const JobId &, uint64_t)> on_progress;
  std::function<void(const JobId &, bool)> on_complete;
};
Result<std::string> checkpoint_sha256(const std::string &path);
VoidResult checkpoint_sync_file(const std::string &path);
class CheckpointManager {
public:
  explicit CheckpointManager(std::string checkpoint_dir);
  CheckpointManager(std::string checkpoint_dir, RuntimeManager &runtime);
  VoidResult take_checkpoint(const JobId &job_id, uint64_t size_hint,
                             const CheckpointCallbacks &cb, bool final = false,
                             int64_t deadline_ns = 0);
  VoidResult restore_checkpoint(const JobId &job_id,
                                const CheckpointCallbacks &cb,
                                int64_t deadline_ns = 0);
  Result<CheckpointMeta> latest_meta(const JobId &job_id) const;
  VoidResult commit_received(CheckpointMeta meta,
                             const std::string &incoming_path);
  VoidResult purge(const JobId &job_id);
  bool exists(const JobId &job_id) const;
  std::string incoming_path(const JobId &job_id, uint32_t sequence) const;
  const std::string &dir() const noexcept { return checkpoint_dir_; }
  RuntimeManager &runtime() { return *runtime_; }

private:
  VoidResult write_meta(const CheckpointMeta &meta);
  std::string meta_path(const JobId &id) const;
  std::string data_path(const JobId &id, uint32_t seq) const;
  std::string checkpoint_dir_;
  std::unique_ptr<RuntimeManager> owned_runtime_;
  RuntimeManager *runtime_;
  std::unordered_map<JobId, std::pair<uint32_t, std::string>> restored_;
  mutable std::recursive_mutex mutex_;
};
}
