#include "reclaimnet/ebpf_loader.h"
#include "../../data_plane/bpf_shared.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <map>
#include <mutex>
#include <time.h>
#if defined(__linux__) && RECLAIMNET_HAVE_LIBBPF
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <net/if.h>
#define RN_LIBBPF 1
#else
#define RN_LIBBPF 0
#endif
namespace reclaimnet::agent {
uint64_t ebpf_flow_key(const FiveTuple &tuple) {
  flow_key key{};
  key.src_ip = tuple.src_ip;
  key.dst_ip = tuple.dst_ip;
  key.src_port = tuple.src_port;
  key.dst_port = tuple.dst_port;
  key.protocol = tuple.protocol;
  return rn_flow_hash(&key);
}
uint64_t ebpf_rate_bytes_per_second(uint64_t bits_per_second) {
  return bits_per_second / 8;
}
#if RN_LIBBPF
struct EbpfResources {
  bpf_object *object = nullptr;
  int ifindex = 0;
  bool hook_created = false;
  bool hook_prepared = false;
  std::vector<std::pair<bpf_tc_attach_point, bpf_tc_opts>> attached;
  void detach() {
    for (const auto &item : attached) {
      bpf_tc_hook hook{};
      hook.sz = sizeof(hook);
      hook.ifindex = ifindex;
      hook.attach_point = item.first;
      bpf_tc_opts options{};
      options.sz = sizeof(options);
      options.handle = item.second.handle;
      options.priority = item.second.priority;
      bpf_tc_detach(&hook, &options);
    }
    attached.clear();
    if (hook_created) {
      bpf_tc_hook hook{};
      hook.sz = sizeof(hook);
      hook.ifindex = ifindex;
      hook.attach_point =
          static_cast<bpf_tc_attach_point>(BPF_TC_INGRESS | BPF_TC_EGRESS);
      bpf_tc_hook_destroy(&hook);
      hook_created = false;
    }
  }
  ~EbpfResources() {
    detach();
    if (object)
      bpf_object__close(object);
  }
};
namespace {
std::mutex loaded_mutex;
std::map<std::string, std::vector<std::shared_ptr<EbpfResources>>> loaded;
std::map<std::string, int> class_maps;
Error syscall_error(const std::string &operation, int status = 0) {
  return Error{operation + ": " + std::strerror(status < 0 ? -status : errno),
               status};
}
uint64_t mono_ns() {
  timespec value{};
  clock_gettime(CLOCK_MONOTONIC, &value);
  return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}
VoidResult attach(const std::shared_ptr<EbpfResources> &resource,
                  const char *program_name, bpf_tc_attach_point point,
                  uint32_t priority, uint32_t handle) {
  auto *program =
      bpf_object__find_program_by_name(resource->object, program_name);
  if (!program)
    return Error{std::string("BPF program not found: ") + program_name};
  bpf_tc_hook hook{};
  hook.sz = sizeof(hook);
  hook.ifindex = resource->ifindex;
  hook.attach_point = point;
  if (!resource->hook_prepared) {
    const int created = bpf_tc_hook_create(&hook);
    if (created && created != -EEXIST)
      return syscall_error("bpf_tc_hook_create", created);
    if (!created)
      resource->hook_created = true;
    resource->hook_prepared = true;
  }
  bpf_tc_opts options{};
  options.sz = sizeof(options);
  options.prog_fd = bpf_program__fd(program);
  options.handle = handle;
  options.priority = priority;
  const int status = bpf_tc_attach(&hook, &options);
  if (status)
    return syscall_error("bpf_tc_attach", status);
  resource->attached.push_back({point, options});
  return ok_void();
}
Result<std::shared_ptr<EbpfResources>> open_object(const std::string &path,
                                                   const std::string &iface) {
  auto resource = std::make_shared<EbpfResources>();
  resource->ifindex = static_cast<int>(if_nametoindex(iface.c_str()));
  resource->hook_prepared = loaded.find(iface) != loaded.end();
  if (!resource->ifindex)
    return syscall_error("if_nametoindex");
  resource->object = bpf_object__open_file(path.c_str(), nullptr);
  const long status = libbpf_get_error(resource->object);
  if (status) {
    resource->object = nullptr;
    return syscall_error("bpf_object__open_file", static_cast<int>(status));
  }
  return resource;
}
VoidResult set_rate(const EbpfHandle &handle, uint64_t key, uint64_t bps,
                    TrafficClass traffic_class) {
  rn_rate_config state{};
  state.rate_bytes_per_sec = ebpf_rate_bytes_per_second(bps);
  state.traffic_class = static_cast<uint32_t>(traffic_class);
  if (bpf_map_update_elem(handle.map_fd, &key, &state, BPF_ANY))
    return syscall_error("flow rate update");
  uint32_t value = static_cast<uint32_t>(traffic_class);
  if (bpf_map_update_elem(handle.classes_fd, &key, &value, BPF_ANY))
    return syscall_error("flow class update");
  return ok_void();
}
}
Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string &path,
                                          const std::string &iface) {
  std::lock_guard<std::mutex> lock(loaded_mutex);
  auto opened = open_object(path, iface);
  if (!opened)
    return opened.error();
  auto resource = opened.value();
  int status = bpf_object__load(resource->object);
  if (status)
    return syscall_error("bpf_object__load", status);
  EbpfHandle handle;
  handle.ports_fd =
      bpf_object__find_map_fd_by_name(resource->object, "traffic_ports_map");
  auto ports = ebpf_set_traffic_ports(handle, 50052, 7600);
  if (!ports)
    return ports.error();
  auto egress = attach(resource, "tc_ratelimit", BPF_TC_EGRESS, 20, 1);
  if (!egress)
    return egress.error();
  auto ingress = attach(resource, "tc_flow_ingress", BPF_TC_INGRESS, 20, 2);
  if (!ingress)
    return ingress.error();
  handle.tc_prog_fd = bpf_program__fd(
      bpf_object__find_program_by_name(resource->object, "tc_ratelimit"));
  handle.map_fd =
      bpf_object__find_map_fd_by_name(resource->object, "flow_rates");
  handle.classes_fd =
      bpf_object__find_map_fd_by_name(resource->object, "flow_classes");
  handle.counters_fd =
      bpf_object__find_map_fd_by_name(resource->object, "flow_counters");
  handle.buckets_fd =
      bpf_object__find_map_fd_by_name(resource->object, "flow_buckets");
  handle.iface = iface;
  handle.resources = resource;
  loaded[iface].push_back(resource);
  class_maps[iface] = handle.classes_fd;
  return handle;
}
VoidResult ebpf_set_flow_rate(const EbpfHandle &handle, uint64_t key,
                              uint64_t bps) {
  return set_rate(handle, key, bps, TrafficClass::PLANNED_MIGRATION);
}
VoidResult ebpf_set_traffic_ports(const EbpfHandle &handle, uint16_t control,
                                  uint16_t migration, uint16_t coordinator) {
  uint32_t key = 0;
  traffic_ports ports{coordinator, control, migration, 0};
  if (bpf_map_update_elem(handle.ports_fd, &key, &ports, BPF_ANY))
    return syscall_error("traffic port update");
  return ok_void();
}
VoidResult ebpf_set_flow_policy(const EbpfHandle &handle,
                                const FiveTuple &tuple, uint64_t bps,
                                TrafficClass traffic_class) {
  return set_rate(handle, ebpf_flow_key(tuple), bps, traffic_class);
}
VoidResult ebpf_remove_flow_policy(const EbpfHandle &handle,
                                   const FiveTuple &tuple) {
  uint64_t key = ebpf_flow_key(tuple);
  if (bpf_map_delete_elem(handle.map_fd, &key) && errno != ENOENT)
    return syscall_error("flow rate removal");
  if (bpf_map_delete_elem(handle.classes_fd, &key) && errno != ENOENT)
    return syscall_error("flow class removal");
  if (bpf_map_delete_elem(handle.buckets_fd, &key) && errno != ENOENT)
    return syscall_error("flow bucket removal");
  return ok_void();
}
Result<std::vector<EbpfFlowStats>>
ebpf_read_flow_stats(const EbpfHandle &handle) {
  std::vector<EbpfFlowStats> out;
  uint64_t previous = 0, next = 0;
  bool first = true;
  const uint64_t now = mono_ns();
  while (bpf_map_get_next_key(handle.counters_fd, first ? nullptr : &previous,
                              &next) == 0) {
    flow_counter counter{};
    if (bpf_map_lookup_elem_flags(handle.counters_fd, &next, &counter,
                                  BPF_F_LOCK) != 0)
      return syscall_error("flow counter lookup");
    EbpfFlowStats stats;
    stats.key = {counter.key.src_ip, counter.key.dst_ip, counter.key.src_port,
                 counter.key.dst_port, counter.key.protocol};
    stats.bytes_tx = counter.bytes_tx;
    stats.bytes_rx = counter.bytes_rx;
    stats.packets_tx = counter.packets_tx;
    stats.packets_rx = counter.packets_rx;
    stats.dropped = counter.dropped;
    stats.sampled_mono_ns = static_cast<int64_t>(counter.last_ts_ns);
    if (now - counter.last_ts_ns <= RECLAIMNET_STATS_INTERVAL_NS) {
      stats.tx_rate_bps = counter.tx_bytes_per_sec * 8.0;
      stats.rx_rate_bps = counter.rx_bytes_per_sec * 8.0;
    }
    out.push_back(stats);
    previous = next;
    first = false;
  }
  if (errno != ENOENT)
    return syscall_error("flow counter iteration");
  return out;
}
Result<int> ebpf_load_kill_switch(const std::string &path,
                                  const std::string &iface) {
  std::lock_guard<std::mutex> lock(loaded_mutex);
  auto opened = open_object(path, iface);
  if (!opened)
    return opened.error();
  auto resource = opened.value();
  auto classes = class_maps.find(iface);
  if (classes != class_maps.end()) {
    auto map = bpf_object__find_map_by_name(resource->object, "flow_classes");
    int status = bpf_map__reuse_fd(map, classes->second);
    if (status)
      return syscall_error("reuse flow class map", status);
  }
  int status = bpf_object__load(resource->object);
  if (status)
    return syscall_error("bpf_object__load", status);
  int map_fd =
      bpf_object__find_map_fd_by_name(resource->object, "kill_switch_map");
  auto policy = ebpf_set_reclaim_policy(map_fd, false, false, 0);
  if (!policy)
    return policy.error();
  auto result = attach(resource, "kill_switch", BPF_TC_INGRESS, 10, 3);
  if (!result)
    return result.error();
  loaded[iface].push_back(resource);
  return map_fd;
}
VoidResult ebpf_set_reclaim_policy(int fd, bool active, bool scheduled,
                                   uint64_t deadline, uint16_t control,
                                   uint16_t migration) {
  uint32_t key = 0;
  rn_reclaim_state state{};
  state.active = active;
  state.scheduled = scheduled;
  state.grace_deadline_mono_ns = deadline;
  state.control_port = control;
  state.migration_port = migration;
  if (bpf_map_update_elem(fd, &key, &state, BPF_ANY))
    return syscall_error("reclaim policy update");
  return ok_void();
}
VoidResult ebpf_set_kill_switch(int fd, bool active) {
  return ebpf_set_reclaim_policy(fd, active, false, 0);
}
void ebpf_unload_all(const std::string &iface) {
  std::lock_guard<std::mutex> lock(loaded_mutex);
  auto found = loaded.find(iface);
  if (found != loaded.end()) {
    for (auto it = found->second.rbegin(); it != found->second.rend(); ++it)
      (*it)->detach();
    loaded.erase(found);
  }
  class_maps.erase(iface);
}
#else
struct EbpfResources {};
Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string &,
                                          const std::string &) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_set_flow_rate(const EbpfHandle &, uint64_t, uint64_t) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_set_flow_policy(const EbpfHandle &, const FiveTuple &, uint64_t,
                                TrafficClass) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_remove_flow_policy(const EbpfHandle &, const FiveTuple &) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_set_traffic_ports(const EbpfHandle &, uint16_t, uint16_t,
                                  uint16_t) {
  return Error{"Linux libbpf TC is unavailable"};
}
Result<std::vector<EbpfFlowStats>> ebpf_read_flow_stats(const EbpfHandle &) {
  return Error{"Linux libbpf TC is unavailable"};
}
Result<int> ebpf_load_kill_switch(const std::string &, const std::string &) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_set_reclaim_policy(int, bool, bool, uint64_t, uint16_t,
                                   uint16_t) {
  return Error{"Linux libbpf TC is unavailable"};
}
VoidResult ebpf_set_kill_switch(int, bool) {
  return Error{"Linux libbpf TC is unavailable"};
}
void ebpf_unload_all(const std::string &) {}
#endif
VoidResult ebpf_remove_flow(const EbpfHandle &handle, const FiveTuple &tuple) {
  return ebpf_remove_flow_policy(handle, tuple);
}
}
