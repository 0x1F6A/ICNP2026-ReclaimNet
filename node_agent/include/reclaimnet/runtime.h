#pragma once
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
namespace reclaimnet::agent {
struct JobRuntimeSpec {
  JobId job_id;
  std::string state_path;
  std::string backend = "file";
  std::string application_module;
  std::string bridge_directory;
  std::string container_name;
  std::string container_image;
  std::string gpu_devices;
  std::string seccomp_profile = "docker/seccomp.json";
  uint64_t memory_limit_bytes = 0;
  bool state_directory = false;
  bool defer_start = false;
  std::vector<std::string> container_args;
  std::vector<std::string> checkpoint_command;
  std::vector<std::string> restore_command;
  std::vector<std::string> start_command;
  std::vector<std::string> pause_command;
  std::vector<std::string> resume_command;
  std::vector<std::string> stop_command;
};
using RuntimeCleanupCb = std::function<VoidResult(const JobRuntimeSpec &)>;
class RuntimeManager {
public:
  explicit RuntimeManager(RuntimeCleanupCb cleanup = {});
  VoidResult register_job(JobRuntimeSpec spec);
  Result<JobRuntimeSpec> spec(const JobId &job_id) const;
  Result<uint64_t> state_size(const JobId &job_id) const;
  VoidResult capture(const JobId &job_id, const std::string &checkpoint_path,
                     bool final = false, int64_t deadline_ns = 0);
  VoidResult restore(const JobId &job_id, const std::string &checkpoint_path,
                     int64_t deadline_ns = 0);
  VoidResult start(const JobId &job_id, int64_t deadline_ns = 0);
  VoidResult pause(const JobId &job_id, int64_t deadline_ns = 0);
  VoidResult resume(const JobId &job_id, int64_t deadline_ns = 0);
  VoidResult stop(const JobId &job_id, int64_t deadline_ns = 0);

private:
  VoidResult control(const JobId &job_id, const std::string &action,
                     int64_t deadline_ns);
  mutable std::recursive_mutex mutex_;
  std::unordered_map<JobId, JobRuntimeSpec> jobs_;
  std::unordered_set<JobId> stopped_;
  RuntimeCleanupCb cleanup_;
};
VoidResult run_runtime_command(const std::vector<std::string> &command,
                               const JobRuntimeSpec &spec,
                               const std::string &checkpoint_path = "",
                               int64_t deadline_ns = 0);
VoidResult python_application_checkpoint(const JobRuntimeSpec &spec,
                                         const std::string &path,
                                         bool restoring);
Result<uint64_t> python_application_state_size(const JobRuntimeSpec &spec);
VoidResult python_application_control(const JobRuntimeSpec &spec,
                                      const std::string &action);
}
