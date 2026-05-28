#pragma once
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
namespace reclaimnet::agent {
struct GpuDevice {
  unsigned index = 0;
  std::string uuid;
  std::string name;
  uint64_t total_vram_bytes = 0;
  uint64_t free_vram_bytes = 0;
  double utilization_pct = 0;
  double cuda_capability = 0;
  bool mig_supported = false;
  bool mig_enabled = false;
};
struct GpuReservation {
  JobId job_id;
  unsigned gpu_index = 0;
  std::string device_uuid;
  uint64_t vram_bytes = 0;
  bool mig = false;
};
class GpuBackend {
public:
  virtual ~GpuBackend() = default;
  virtual Result<std::vector<GpuDevice>> inventory() = 0;
  virtual Result<std::string> create_mig(unsigned index, unsigned gpu_profile,
                                         unsigned compute_profile,
                                         uint64_t required_bytes) = 0;
  virtual VoidResult destroy_mig(const std::string &uuid) = 0;
  virtual Result<uint64_t> zero_free_vram(const std::string &uuid) = 0;
};
class GpuIsolation {
public:
  explicit GpuIsolation(std::shared_ptr<GpuBackend> backend = {});
  Result<std::vector<GpuDevice>> inventory();
  Result<GpuReservation> reserve(const JobId &job, unsigned gpu_index,
                                 uint64_t vram_bytes, int mig_profile = -1,
                                 unsigned compute_profile = 0);
  VoidResult release(const JobId &job, bool zero_vram = true);
  std::vector<GpuReservation> reservations() const;
  Result<uint64_t> zero_device(unsigned gpu_index);

private:
  std::shared_ptr<GpuBackend> backend_;
  mutable std::mutex mutex_;
  std::unordered_map<JobId, std::vector<GpuReservation>> reservations_;
};
}
