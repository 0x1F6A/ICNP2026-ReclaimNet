#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <string>
#include <functional>
#include <cstdint>

namespace reclaimnet {
namespace agent {


struct CheckpointMeta {
  JobId    job_id;
  uint32_t sequence;
  uint64_t size_bytes;
  int64_t  timestamp_ns;
  std::string path;
  bool     complete;
};


struct CheckpointCallbacks {
  std::function<void(const JobId&, uint64_t bytes_written)> on_progress;
  std::function<void(const JobId&, bool success)>           on_complete;
};


class CheckpointManager {
public:
  explicit CheckpointManager(std::string checkpoint_dir);




  VoidResult take_checkpoint(const JobId& job_id,
                              uint64_t vram_bytes,
                              const CheckpointCallbacks& cb);


  VoidResult restore_checkpoint(const JobId& job_id,
                                 const CheckpointCallbacks& cb);


  Result<CheckpointMeta> latest_meta(const JobId& job_id) const;


  VoidResult purge(const JobId& job_id);


  bool exists(const JobId& job_id) const;

  const std::string& dir() const noexcept { return checkpoint_dir_; }

private:
  std::string checkpoint_dir_;

  std::string meta_path(const JobId& id) const;
  std::string data_path(const JobId& id, uint32_t seq) const;
};

}
}
