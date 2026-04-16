


#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <cstring>

namespace fs = std::filesystem;

namespace reclaimnet {
namespace agent {



CheckpointManager::CheckpointManager(std::string checkpoint_dir)
  : checkpoint_dir_(std::move(checkpoint_dir)) {
  fs::create_directories(checkpoint_dir_);
}



std::string CheckpointManager::meta_path(const JobId& id) const {
  return checkpoint_dir_ + "/" + id + ".meta";
}

std::string CheckpointManager::data_path(const JobId& id, uint32_t seq) const {
  return checkpoint_dir_ + "/" + id + "." + std::to_string(seq) + ".ckpt";
}



bool CheckpointManager::exists(const JobId& id) const {
  return fs::exists(meta_path(id));
}



Result<CheckpointMeta> CheckpointManager::latest_meta(const JobId& id) const {
  std::ifstream ifs(meta_path(id));
  if (!ifs.is_open()) return Error{"No checkpoint for job " + id};

  CheckpointMeta m{};
  m.job_id = id;
  std::string line;
  while (std::getline(ifs, line)) {
    auto sep = line.find('=');
    if (sep == std::string::npos) continue;
    auto k = line.substr(0, sep);
    auto v = line.substr(sep + 1);
    if      (k == "seq")       m.sequence   = (uint32_t)std::stoul(v);
    else if (k == "size")      m.size_bytes = (uint64_t)std::stoull(v);
    else if (k == "ts_ns")     m.timestamp_ns = (int64_t)std::stoll(v);
    else if (k == "path")      m.path        = v;
    else if (k == "complete")  m.complete    = (v == "1");
  }
  return m;
}



VoidResult CheckpointManager::take_checkpoint(
    const JobId& job_id,
    uint64_t vram_bytes,
    const CheckpointCallbacks& cb) {


  uint32_t seq = 0;
  if (exists(job_id)) {
    auto prev = latest_meta(job_id);
    if (prev) seq = prev.value().sequence + 1;
  }

  std::string path = data_path(job_id, seq);
  LOG_INFO("CheckpointManager: writing %s seq=%u size=%llu bytes",
           job_id.c_str(), seq, (unsigned long long)vram_bytes);


  {
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) return Error{"Cannot create checkpoint: " + path};


    const size_t CHUNK = 65536;
    std::vector<char> buf(CHUNK, 0xAB);
    uint64_t written = 0;
    while (written < vram_bytes) {
      size_t n = std::min((uint64_t)CHUNK, vram_bytes - written);
      ofs.write(buf.data(), (std::streamsize)n);
      written += n;
      if (cb.on_progress) cb.on_progress(job_id, written);
    }
  }


  {
    std::ofstream ofs(meta_path(job_id), std::ios::trunc);
    ofs << "seq="      << seq       << "\n"
        << "size="     << vram_bytes<< "\n"
        << "ts_ns="    << now_ns()  << "\n"
        << "path="     << path      << "\n"
        << "complete=1\n";
  }

  LOG_INFO("CheckpointManager: checkpoint done for %s", job_id.c_str());
  if (cb.on_complete) cb.on_complete(job_id, true);
  return ok_void();
}



VoidResult CheckpointManager::restore_checkpoint(
    const JobId& job_id, const CheckpointCallbacks& cb) {

  auto meta_res = latest_meta(job_id);
  if (!meta_res) return meta_res.error();
  const CheckpointMeta& meta = meta_res.value();

  if (!meta.complete) return Error{"Checkpoint incomplete for " + job_id};
  if (!fs::exists(meta.path)) return Error{"Checkpoint file missing: " + meta.path};


  std::ifstream ifs(meta.path, std::ios::binary);
  if (!ifs.is_open()) return Error{"Cannot open checkpoint: " + meta.path};

  const size_t CHUNK = 65536;
  std::vector<char> buf(CHUNK);
  uint64_t read_bytes = 0;
  while (ifs.read(buf.data(), CHUNK) || ifs.gcount() > 0) {
    read_bytes += (uint64_t)ifs.gcount();
    if (cb.on_progress) cb.on_progress(job_id, read_bytes);
  }

  LOG_INFO("CheckpointManager: restored %s (%llu bytes)",
           job_id.c_str(), (unsigned long long)read_bytes);
  if (cb.on_complete) cb.on_complete(job_id, true);
  return ok_void();
}



VoidResult CheckpointManager::purge(const JobId& job_id) {
  auto meta_res = latest_meta(job_id);
  if (meta_res) {
    fs::remove(meta_res.value().path);
  }
  fs::remove(meta_path(job_id));
  LOG_DEBUG("CheckpointManager: purged checkpoint for %s", job_id.c_str());
  return ok_void();
}

}
}
