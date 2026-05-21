#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include <array>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unistd.h>
namespace reclaimnet::agent {
namespace fs = std::filesystem;
namespace {
bool valid_id(const std::string &id) {
  return !id.empty() && id != "." && id != ".." &&
         id.find_first_of("/\\\n\r") == std::string::npos;
}
VoidResult sync_directory(const std::string &directory) {
  int fd = open(directory.c_str(), O_RDONLY);
  if (fd < 0)
    return Error{"Cannot open checkpoint directory"};
  int result = fsync(fd);
  close(fd);
  return result == 0 ? ok_void() : err_void("Cannot sync checkpoint directory");
}
}
VoidResult checkpoint_sync_file(const std::string &path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return Error{"Cannot open checkpoint for sync: " + path};
  int result = fsync(fd);
  close(fd);
  return result == 0 ? ok_void() : err_void("Cannot sync checkpoint: " + path);
}
Result<std::string> checkpoint_sha256(const std::string &path) {
  return sha256_file(path);
}
CheckpointManager::CheckpointManager(std::string directory)
    : checkpoint_dir_(std::move(directory)),
      owned_runtime_(std::make_unique<RuntimeManager>()),
      runtime_(owned_runtime_.get()) {
  fs::create_directories(checkpoint_dir_);
}
CheckpointManager::CheckpointManager(std::string directory,
                                     RuntimeManager &runtime)
    : checkpoint_dir_(std::move(directory)), runtime_(&runtime) {
  fs::create_directories(checkpoint_dir_);
}
std::string CheckpointManager::meta_path(const JobId &id) const {
  return checkpoint_dir_ + "/" + id + ".meta";
}
std::string CheckpointManager::data_path(const JobId &id, uint32_t seq) const {
  return checkpoint_dir_ + "/" + id + "." + std::to_string(seq) + ".ckpt";
}
std::string CheckpointManager::incoming_path(const JobId &id,
                                             uint32_t seq) const {
  if (!valid_id(id))
    return "";
  return data_path(id, seq) + ".partial";
}
bool CheckpointManager::exists(const JobId &id) const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  return valid_id(id) && fs::exists(meta_path(id));
}
Result<CheckpointMeta> CheckpointManager::latest_meta(const JobId &id) const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  if (!valid_id(id))
    return Error{"Invalid checkpoint job id"};
  std::ifstream input(meta_path(id));
  if (!input)
    return Error{"No committed checkpoint for job " + id};
  CheckpointMeta meta;
  meta.job_id = id;
  std::string line;
  try {
    while (std::getline(input, line)) {
      auto sep = line.find('=');
      if (sep == std::string::npos)
        continue;
      auto key = line.substr(0, sep), value = line.substr(sep + 1);
      if (key == "seq")
        meta.sequence = std::stoul(value);
      else if (key == "size")
        meta.size_bytes = std::stoull(value);
      else if (key == "ts_ns")
        meta.timestamp_ns = std::stoll(value);
      else if (key == "path")
        meta.path = value;
      else if (key == "complete")
        meta.complete = value == "1";
      else if (key == "sha256")
        meta.sha256 = value;
      else if (key == "restored")
        meta.restored = value == "1";
      else if (key == "transfer_id")
        meta.transfer_id = value;
      else if (key == "rank")
        meta.rank = std::stoul(value);
    }
  } catch (const std::exception &error) {
    return Error{error.what()};
  }
  if (!meta.complete || meta.sha256.size() != 64 ||
      meta.path != data_path(id, meta.sequence))
    return Error{"Incomplete checkpoint metadata"};
  return meta;
}
VoidResult CheckpointManager::write_meta(const CheckpointMeta &meta) {
  const auto temporary = meta_path(meta.job_id) + ".tmp";
  {
    std::ofstream output(temporary, std::ios::trunc);
    output << "seq=" << meta.sequence << "\nsize=" << meta.size_bytes
           << "\nts_ns=" << meta.timestamp_ns << "\npath=" << meta.path
           << "\ncomplete=" << meta.complete << "\nsha256=" << meta.sha256
           << "\nrestored=" << meta.restored
           << "\ntransfer_id=" << meta.transfer_id << "\nrank=" << meta.rank
           << "\n";
    output.flush();
    if (!output)
      return Error{"Checkpoint metadata write failed"};
  }
  if (auto result = checkpoint_sync_file(temporary); !result)
    return result;
  std::error_code error;
  fs::rename(temporary, meta_path(meta.job_id), error);
  if (error)
    return Error{error.message()};
  return sync_directory(checkpoint_dir_);
}
VoidResult CheckpointManager::take_checkpoint(const JobId &id, uint64_t,
                                              const CheckpointCallbacks &cb,
                                              bool final, int64_t deadline_ns) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  if (!valid_id(id))
    return Error{"Invalid checkpoint job id"};
  uint32_t seq = 0;
  if (exists(id)) {
    auto previous = latest_meta(id);
    if (!previous)
      return previous.error();
    seq = previous.value().sequence + 1;
  }
  auto temporary = data_path(id, seq) + ".capture";
  if (auto result = runtime_->capture(id, temporary, final, deadline_ns);
      !result)
    return result;
  auto hash = checkpoint_sha256(temporary);
  if (!hash) {
    if (final)
      runtime_->resume(id);
    return hash.error();
  }
  CheckpointMeta meta{id,           seq,  fs::file_size(temporary),
                      now_ns(),     "",   true,
                      hash.value(), false};
  if (deadline_ns > 0 && now_ns() >= deadline_ns) {
    if (final)
      runtime_->resume(id);
    return Error{"Checkpoint capture deadline exceeded"};
  }
  auto result = commit_received(meta, temporary);
  if (!result) {
    if (final)
      runtime_->resume(id);
    return result;
  }
  if (deadline_ns > 0 && now_ns() >= deadline_ns) {
    if (final)
      runtime_->resume(id);
    return Error{"Checkpoint commit deadline exceeded"};
  }
  if (cb.on_progress)
    cb.on_progress(id, meta.size_bytes);
  if (cb.on_complete)
    cb.on_complete(id, true);
  return ok_void();
}
VoidResult CheckpointManager::commit_received(CheckpointMeta meta,
                                              const std::string &incoming) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  if (!valid_id(meta.job_id) || !meta.complete || meta.sha256.size() != 64)
    return Error{"Invalid checkpoint commit metadata"};
  if (exists(meta.job_id)) {
    auto previous = latest_meta(meta.job_id);
    if (!previous)
      return previous.error();
    if (meta.sequence < previous.value().sequence)
      return Error{"An older checkpoint cannot replace the committed state"};
    if (meta.sequence == previous.value().sequence) {
      if (meta.sha256 != previous.value().sha256 ||
          meta.size_bytes != previous.value().size_bytes)
        return Error{"Checkpoint sequence identifies conflicting state"};
      auto committed_hash = checkpoint_sha256(previous.value().path);
      if (!committed_hash)
        return committed_hash.error();
      if (committed_hash.value() != meta.sha256 ||
          fs::file_size(previous.value().path) != meta.size_bytes)
        return Error{"Committed checkpoint integrity failed"};
      return ok_void();
    }
  }
  std::error_code error;
  if (fs::file_size(incoming, error) != meta.size_bytes || error)
    return Error{"Checkpoint payload size differs from metadata"};
  auto hash = checkpoint_sha256(incoming);
  if (!hash)
    return hash.error();
  if (hash.value() != meta.sha256)
    return Error{"Checkpoint checksum mismatch"};
  if (auto result = checkpoint_sync_file(incoming); !result)
    return result;
  meta.path = data_path(meta.job_id, meta.sequence);
  if (incoming != meta.path)
    fs::rename(incoming, meta.path, error);
  if (error)
    return Error{"Checkpoint commit failed: " + error.message()};
  if (auto result = sync_directory(checkpoint_dir_); !result)
    return result;
  return write_meta(meta);
}
VoidResult CheckpointManager::restore_checkpoint(const JobId &id,
                                                 const CheckpointCallbacks &cb,
                                                 int64_t deadline_ns) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto result = latest_meta(id);
  if (!result)
    return result.error();
  auto meta = result.value();
  auto hash = checkpoint_sha256(meta.path);
  if (!hash)
    return hash.error();
  if (fs::file_size(meta.path) != meta.size_bytes ||
      hash.value() != meta.sha256)
    return Error{"Committed checkpoint integrity failed"};
  auto restored = restored_.find(id);
  if (restored == restored_.end() || restored->second.first != meta.sequence ||
      restored->second.second != meta.sha256) {
    if (auto restored = runtime_->restore(id, meta.path, deadline_ns);
        !restored)
      return restored;
    meta.restored = true;
    if (auto saved = write_meta(meta); !saved)
      return saved;
    restored_[id] = {meta.sequence, meta.sha256};
  }
  if (deadline_ns > 0 && now_ns() >= deadline_ns)
    return Error{"Checkpoint restore deadline exceeded"};
  if (cb.on_progress)
    cb.on_progress(id, meta.size_bytes);
  if (cb.on_complete)
    cb.on_complete(id, true);
  return ok_void();
}
VoidResult CheckpointManager::purge(const JobId &id) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto meta = latest_meta(id);
  if (!meta)
    return meta.error();
  const auto prefix = id + ".";
  for (const auto &entry : fs::directory_iterator(checkpoint_dir_)) {
    const auto name = entry.path().filename().string();
    if (name.rfind(prefix, 0) != 0)
      continue;
    const auto end = name.find('.', prefix.size());
    if (end == std::string::npos || end == prefix.size())
      continue;
    const auto sequence = name.substr(prefix.size(), end - prefix.size());
    if (sequence.find_first_not_of("0123456789") != std::string::npos)
      continue;
    const auto suffix = name.substr(end);
    if (suffix == ".ckpt" || suffix == ".ckpt.partial" ||
        suffix == ".ckpt.partial.identity" || suffix == ".ckpt.capture")
      fs::remove(entry.path());
  }
  fs::remove(meta_path(id));
  restored_.erase(id);
  return sync_directory(checkpoint_dir_);
}
}
