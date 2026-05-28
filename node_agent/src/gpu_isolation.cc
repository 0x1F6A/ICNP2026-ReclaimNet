#include "reclaimnet/gpu_isolation.h"
#include <algorithm>
#include <array>
#include <dlfcn.h>
#include <iomanip>
#include <sstream>
#include <utility>
namespace reclaimnet::agent {
namespace {
struct NvmlMemory {
  unsigned long long total, free, used;
};
struct NvmlUtilization {
  unsigned gpu, memory;
};
struct NvmlGpuInfo {
  void *device;
  unsigned id, profile;
  unsigned start, size;
};
struct NvmlGpuProfile {
  unsigned fields[10];
  unsigned long long memory_mb;
};
struct CudaUuid {
  unsigned char bytes[16];
};
class NativeGpuBackend final : public GpuBackend {
  void *nvml_ = nullptr;
  void *cuda_ = nullptr;
  bool initialized_ = false;
  std::unordered_map<std::string, std::pair<void *, void *>> instances_;
  template <typename T> T symbol(void *library, const char *name) {
    return library ? reinterpret_cast<T>(dlsym(library, name)) : nullptr;
  }
  Error nvml_error(int code) {
    auto message = symbol<const char *(*)(int)>(nvml_, "nvmlErrorString");
    return Error{message ? message(code) : "NVML operation failed", code};
  }
  Result<void *> device(unsigned index) {
    auto get = symbol<int (*)(unsigned, void **)>(
        nvml_, "nvmlDeviceGetHandleByIndex_v2");
    void *result = nullptr;
    if (!get)
      return Error{"NVML device interface is unavailable"};
    int status = get(index, &result);
    if (status)
      return nvml_error(status);
    return result;
  }

public:
  NativeGpuBackend() {
    nvml_ = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
    cuda_ = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
    auto init = symbol<int (*)()>(nvml_, "nvmlInit_v2");
    initialized_ = init && init() == 0;
  }
  ~NativeGpuBackend() override {
    for (const auto &instance : instances_) {
      auto ci = symbol<int (*)(void *)>(nvml_, "nvmlComputeInstanceDestroy");
      auto gi = symbol<int (*)(void *)>(nvml_, "nvmlGpuInstanceDestroy");
      if (ci && instance.second.second)
        ci(instance.second.second);
      if (gi)
        gi(instance.second.first);
    }
    auto shutdown = symbol<int (*)()>(nvml_, "nvmlShutdown");
    if (initialized_ && shutdown)
      shutdown();
    if (cuda_)
      dlclose(cuda_);
    if (nvml_)
      dlclose(nvml_);
  }
  Result<std::vector<GpuDevice>> inventory() override {
    if (!initialized_)
      return Error{"NVML is unavailable; no NVIDIA inventory can be collected"};
    auto count = symbol<int (*)(unsigned *)>(nvml_, "nvmlDeviceGetCount_v2");
    auto name =
        symbol<int (*)(void *, char *, unsigned)>(nvml_, "nvmlDeviceGetName");
    auto uuid =
        symbol<int (*)(void *, char *, unsigned)>(nvml_, "nvmlDeviceGetUUID");
    auto memory =
        symbol<int (*)(void *, NvmlMemory *)>(nvml_, "nvmlDeviceGetMemoryInfo");
    auto util = symbol<int (*)(void *, NvmlUtilization *)>(
        nvml_, "nvmlDeviceGetUtilizationRates");
    auto capability = symbol<int (*)(void *, int *, int *)>(
        nvml_, "nvmlDeviceGetCudaComputeCapability");
    auto mig_mode = symbol<int (*)(void *, unsigned *, unsigned *)>(
        nvml_, "nvmlDeviceGetMigMode");
    if (!count || !name || !uuid || !memory)
      return Error{"NVML inventory symbols are unavailable"};
    unsigned devices = 0;
    int status = count(&devices);
    if (status)
      return nvml_error(status);
    std::vector<GpuDevice> inventory;
    for (unsigned i = 0; i < devices; ++i) {
      auto d = device(i);
      if (!d)
        return d.error();
      std::array<char, 96> n{}, id{};
      NvmlMemory m{};
      NvmlUtilization u{};
      if ((status = name(d.value(), n.data(), n.size())) ||
          (status = uuid(d.value(), id.data(), id.size())) ||
          (status = memory(d.value(), &m)))
        return nvml_error(status);
      GpuDevice out;
      out.index = i;
      out.name = n.data();
      out.uuid = id.data();
      out.total_vram_bytes = m.total;
      out.free_vram_bytes = m.free;
      if (util && util(d.value(), &u) == 0)
        out.utilization_pct = u.gpu;
      int major = 0, minor = 0;
      if (capability && capability(d.value(), &major, &minor) == 0)
        out.cuda_capability = major + minor / 10.0;
      unsigned current = 0, pending = 0;
      out.mig_supported =
          mig_mode && mig_mode(d.value(), &current, &pending) == 0;
      out.mig_enabled = current == 1;
      inventory.push_back(std::move(out));
    }
    return inventory;
  }
  Result<std::string> create_mig(unsigned index, unsigned profile,
                                 unsigned compute_profile,
                                 uint64_t required_bytes) override {
    auto d = device(index);
    if (!d)
      return d.error();
    auto mode =
        symbol<int (*)(void *, unsigned, int *)>(nvml_, "nvmlDeviceSetMigMode");
    auto create = symbol<int (*)(void *, unsigned, void **)>(
        nvml_, "nvmlDeviceCreateGpuInstance");
    auto create_ci = symbol<int (*)(void *, unsigned, void **)>(
        nvml_, "nvmlGpuInstanceCreateComputeInstance");
    auto info =
        symbol<int (*)(void *, NvmlGpuInfo *)>(nvml_, "nvmlGpuInstanceGetInfo");
    auto max_devices = symbol<int (*)(void *, unsigned *)>(
        nvml_, "nvmlDeviceGetMaxMigDeviceCount");
    auto get_mig = symbol<int (*)(void *, unsigned, void **)>(
        nvml_, "nvmlDeviceGetMigDeviceHandleByIndex");
    auto get_id = symbol<int (*)(void *, unsigned *)>(
        nvml_, "nvmlDeviceGetGpuInstanceId");
    auto uuid =
        symbol<int (*)(void *, char *, unsigned)>(nvml_, "nvmlDeviceGetUUID");
    auto memory =
        symbol<int (*)(void *, NvmlMemory *)>(nvml_, "nvmlDeviceGetMemoryInfo");
    auto destroy_gi = symbol<int (*)(void *)>(nvml_, "nvmlGpuInstanceDestroy");
    auto destroy_ci =
        symbol<int (*)(void *)>(nvml_, "nvmlComputeInstanceDestroy");
    if (!mode || !create || !create_ci || !info || !max_devices || !get_mig ||
        !get_id || !uuid || !memory || !destroy_gi || !destroy_ci)
      return Error{"NVML MIG interfaces are unavailable"};
    int activation = 0;
    int status = mode(d.value(), 1, &activation);
    if (status)
      return nvml_error(status);
    if (activation)
      return nvml_error(activation);
    if (profile == ~0u) {
      auto query = symbol<int (*)(void *, unsigned, NvmlGpuProfile *)>(
          nvml_, "nvmlDeviceGetGpuInstanceProfileInfo");
      auto capacity = symbol<int (*)(void *, unsigned, unsigned *)>(
          nvml_, "nvmlDeviceGetGpuInstanceRemainingCapacity");
      if (!query || !capacity)
        return Error{"NVML MIG profile enumeration is unavailable"};
      unsigned selected = ~0u;
      uint64_t smallest = UINT64_MAX;
      for (unsigned i = 0;; ++i) {
        NvmlGpuProfile candidate{};
        int result = query(d.value(), i, &candidate);
        if (result == 2)
          break;
        if (result == 3)
          continue;
        if (result)
          return nvml_error(result);
        unsigned remaining = 0;
        result = capacity(d.value(), candidate.fields[0], &remaining);
        if (result)
          return nvml_error(result);
        uint64_t bytes = candidate.memory_mb * 1024ULL * 1024ULL;
        if (remaining && bytes >= required_bytes && bytes < smallest) {
          selected = candidate.fields[0];
          smallest = bytes;
        }
      }
      if (selected == ~0u)
        return Error{"No available MIG profile can satisfy the requested VRAM"};
      profile = selected;
    }
    void *gi = nullptr;
    void *ci = nullptr;
    if ((status = create(d.value(), profile, &gi)))
      return nvml_error(status);
    if ((status = create_ci(gi, compute_profile, &ci))) {
      destroy_gi(gi);
      return nvml_error(status);
    }
    NvmlGpuInfo gpu_info{};
    unsigned max_count = 0;
    if ((status = info(gi, &gpu_info)) ||
        (status = max_devices(d.value(), &max_count))) {
      destroy_ci(ci);
      destroy_gi(gi);
      return nvml_error(status);
    }
    for (unsigned i = 0; i < max_count; ++i) {
      void *mig = nullptr;
      unsigned instance_id = 0;
      if (get_mig(d.value(), i, &mig) || get_id(mig, &instance_id) ||
          instance_id != gpu_info.id)
        continue;
      std::array<char, 96> id{};
      if ((status = uuid(mig, id.data(), id.size())))
        break;
      NvmlMemory available{};
      if ((status = memory(mig, &available)))
        break;
      if (available.total < required_bytes) {
        destroy_ci(ci);
        destroy_gi(gi);
        return Error{"MIG partition is smaller than the requested VRAM"};
      }
      std::string value(id.data());
      instances_[value] = {gi, ci};
      return value;
    }
    destroy_ci(ci);
    destroy_gi(gi);
    return Error{"Created MIG device is not visible through NVML"};
  }
  VoidResult destroy_mig(const std::string &uuid) override {
    auto instance = instances_.find(uuid);
    if (instance == instances_.end())
      return Error{"MIG reservation is unknown"};
    auto ci = symbol<int (*)(void *)>(nvml_, "nvmlComputeInstanceDestroy");
    auto gi = symbol<int (*)(void *)>(nvml_, "nvmlGpuInstanceDestroy");
    int status = instance->second.second ? ci(instance->second.second) : 0;
    if (status)
      return nvml_error(status);
    instance->second.second = nullptr;
    status = gi(instance->second.first);
    if (status)
      return nvml_error(status);
    instances_.erase(instance);
    return ok_void();
  }
  Result<uint64_t> zero_free_vram(const std::string &uuid) override {
    auto init = symbol<int (*)(unsigned)>(cuda_, "cuInit");
    auto count = symbol<int (*)(int *)>(cuda_, "cuDeviceGetCount");
    auto get_uuid =
        symbol<int (*)(CudaUuid *, int)>(cuda_, "cuDeviceGetUuid_v2");
    if (!get_uuid)
      get_uuid = symbol<int (*)(CudaUuid *, int)>(cuda_, "cuDeviceGetUuid");
    auto retain =
        symbol<int (*)(void **, int)>(cuda_, "cuDevicePrimaryCtxRetain");
    auto release = symbol<int (*)(int)>(cuda_, "cuDevicePrimaryCtxRelease_v2");
    if (!release)
      release = symbol<int (*)(int)>(cuda_, "cuDevicePrimaryCtxRelease");
    auto set = symbol<int (*)(void *)>(cuda_, "cuCtxSetCurrent");
    auto get = symbol<int (*)(void **)>(cuda_, "cuCtxGetCurrent");
    auto memory = symbol<int (*)(size_t *, size_t *)>(cuda_, "cuMemGetInfo_v2");
    auto allocate =
        symbol<int (*)(unsigned long long *, size_t)>(cuda_, "cuMemAlloc_v2");
    auto zero = symbol<int (*)(unsigned long long, unsigned char, size_t)>(
        cuda_, "cuMemsetD8_v2");
    auto free = symbol<int (*)(unsigned long long)>(cuda_, "cuMemFree_v2");
    auto sync = symbol<int (*)()>(cuda_, "cuCtxSynchronize");
    if (!init || !count || !get_uuid || !retain || !release || !set || !get ||
        !memory || !allocate || !zero || !free || !sync)
      return Error{"CUDA driver zeroing interfaces are unavailable"};
    int status = init(0);
    if (status)
      return Error{"cuInit failed", status};
    int devices = 0;
    if ((status = count(&devices)))
      return Error{"cuDeviceGetCount failed", status};
    int selected = -1;
    for (int i = 0; i < devices; ++i) {
      CudaUuid id{};
      if (get_uuid(&id, i))
        continue;
      std::ostringstream text;
      for (size_t b = 0; b < 16; ++b) {
        if (b == 4 || b == 6 || b == 8 || b == 10)
          text << '-';
        text << std::hex << std::setw(2) << std::setfill('0')
             << static_cast<unsigned>(id.bytes[b]);
      }
      if (uuid == "GPU-" + text.str() || uuid == "MIG-" + text.str()) {
        selected = i;
        break;
      }
    }
    if (selected < 0)
      return Error{"Reserved GPU UUID is not visible to the CUDA driver"};
    void *context = nullptr;
    void *previous = nullptr;
    get(&previous);
    if ((status = retain(&context, selected)))
      return Error{"cuDevicePrimaryCtxRetain failed", status};
    if ((status = set(context))) {
      release(selected);
      return Error{"cuCtxSetCurrent failed", status};
    }
    size_t available = 0, total = 0;
    status = memory(&available, &total);
    std::vector<unsigned long long> pages;
    uint64_t wiped = 0;
    size_t chunk = std::min<size_t>(available, 64 * 1024 * 1024);
    while (!status && available >= 4096 && chunk >= 4096) {
      unsigned long long pointer = 0;
      int allocation_status = allocate(&pointer, std::min(chunk, available));
      if (allocation_status) {
        chunk /= 2;
        continue;
      }
      size_t size = std::min(chunk, available);
      pages.push_back(pointer);
      status = zero(pointer, 0, size);
      if (!status)
        wiped += size;
      available -= size;
    }
    if (!status)
      status = sync();
    for (auto pointer : pages)
      free(pointer);
    set(previous);
    release(selected);
    if (status)
      return Error{"CUDA VRAM zeroing failed", status};
    return wiped;
  }
};
}
GpuIsolation::GpuIsolation(std::shared_ptr<GpuBackend> backend)
    : backend_(backend ? std::move(backend)
                       : std::make_shared<NativeGpuBackend>()) {}
Result<std::vector<GpuDevice>> GpuIsolation::inventory() {
  std::lock_guard<std::mutex> lock(mutex_);
  return backend_->inventory();
}
Result<GpuReservation> GpuIsolation::reserve(const JobId &job, unsigned index,
                                             uint64_t bytes, int mig_profile,
                                             unsigned compute_profile) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (job.empty() || !bytes)
    return Error{"GPU reservation requires job identity and VRAM bytes"};
  for (const auto &entry : reservations_[job])
    if (entry.gpu_index == index) {
      if (entry.vram_bytes == bytes && entry.mig == (mig_profile != -1))
        return entry;
      return Error{"GPU reservation conflicts with the active job allocation"};
    }
  auto devices = backend_->inventory();
  if (!devices)
    return devices.error();
  auto d =
      std::find_if(devices.value().begin(), devices.value().end(),
                   [index](const auto &value) { return value.index == index; });
  if (d == devices.value().end())
    return Error{"GPU index is not present in inventory"};
  uint64_t used = 0;
  for (const auto &group : reservations_)
    for (const auto &entry : group.second)
      if (entry.gpu_index == index)
        used += entry.vram_bytes;
  if (used > d->total_vram_bytes || bytes > d->total_vram_bytes - used ||
      bytes > d->free_vram_bytes)
    return Error{"Insufficient GPU VRAM for reservation"};
  GpuReservation entry{job, index, d->uuid, bytes, mig_profile != -1};
  if (entry.mig) {
    if (!d->mig_supported)
      return Error{"This GPU does not support NVML MIG partitioning"};
    unsigned profile =
        mig_profile == -2 ? ~0u : static_cast<unsigned>(mig_profile);
    auto instance =
        backend_->create_mig(index, profile, compute_profile, bytes);
    if (!instance)
      return instance.error();
    entry.device_uuid = instance.value();
  }
  reservations_[job].push_back(entry);
  return entry;
}
VoidResult GpuIsolation::release(const JobId &job, bool zero_vram) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = reservations_.find(job);
  if (found == reservations_.end())
    return ok_void();
  while (!found->second.empty()) {
    const auto entry = found->second.front();
    if (zero_vram) {
      auto zeroed = backend_->zero_free_vram(entry.device_uuid);
      if (!zeroed)
        return zeroed.error();
    }
    if (entry.mig) {
      auto destroyed = backend_->destroy_mig(entry.device_uuid);
      if (!destroyed)
        return destroyed;
    }
    found->second.erase(found->second.begin());
  }
  reservations_.erase(found);
  return ok_void();
}
std::vector<GpuReservation> GpuIsolation::reservations() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<GpuReservation> out;
  for (const auto &group : reservations_)
    out.insert(out.end(), group.second.begin(), group.second.end());
  return out;
}
Result<uint64_t> GpuIsolation::zero_device(unsigned index) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto devices = backend_->inventory();
  if (!devices)
    return devices.error();
  for (const auto &device : devices.value())
    if (device.index == index)
      return backend_->zero_free_vram(device.uuid);
  return Error{"GPU index is not present in inventory"};
}
}
