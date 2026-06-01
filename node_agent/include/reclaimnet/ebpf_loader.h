#pragma once
#include "reclaimnet/result.h"
#include "reclaimnet/types.h"
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
namespace reclaimnet::agent {
enum class TrafficClass : uint32_t {
  RESEARCH = 0,
  P1_CHECKPOINT = 8,
  PLANNED_MIGRATION = 34,
  EMERGENCY_METADATA = 46
};
struct EbpfResources;
struct EbpfHandle {
  int tc_prog_fd = -1;
  int map_fd = -1;
  std::string iface;
  int classes_fd = -1;
  int counters_fd = -1;
  int ports_fd = -1;
  int buckets_fd = -1;
  std::shared_ptr<EbpfResources> resources;
};
struct EbpfFlowStats {
  FiveTuple key;
  uint64_t bytes_tx = 0;
  uint64_t bytes_rx = 0;
  uint64_t packets_tx = 0;
  uint64_t packets_rx = 0;
  uint64_t dropped = 0;
  int64_t sampled_mono_ns = 0;
  double tx_rate_bps = 0;
  double rx_rate_bps = 0;
};
uint64_t ebpf_flow_key(const FiveTuple &tuple);
uint64_t ebpf_rate_bytes_per_second(uint64_t bits_per_second);
Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string &obj_path,
                                          const std::string &iface);
VoidResult ebpf_set_flow_rate(const EbpfHandle &handle, uint64_t flow_key,
                              uint64_t rate_bps);
VoidResult ebpf_set_flow_policy(const EbpfHandle &handle,
                                const FiveTuple &tuple, uint64_t rate_bps,
                                TrafficClass traffic_class);
VoidResult ebpf_remove_flow_policy(const EbpfHandle &handle,
                                   const FiveTuple &tuple);
VoidResult ebpf_remove_flow(const EbpfHandle &handle, const FiveTuple &tuple);
VoidResult ebpf_set_traffic_ports(const EbpfHandle &handle,
                                  uint16_t control_port,
                                  uint16_t migration_port,
                                  uint16_t coordinator_port = 50051);
Result<std::vector<EbpfFlowStats>>
ebpf_read_flow_stats(const EbpfHandle &handle);
Result<int> ebpf_load_kill_switch(const std::string &obj_path,
                                  const std::string &iface);
VoidResult ebpf_set_kill_switch(int map_fd, bool active);
VoidResult ebpf_set_reclaim_policy(int map_fd, bool active, bool scheduled,
                                   uint64_t grace_deadline_mono_ns,
                                   uint16_t control_port = 50052,
                                   uint16_t migration_port = 7600);
void ebpf_unload_all(const std::string &iface);
}
