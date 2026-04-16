#pragma once





#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <string>
#include <cstdint>

namespace reclaimnet {
namespace agent {

#ifdef __linux__


struct EbpfHandle {
  int tc_prog_fd  = -1;
  int map_fd      = -1;
  std::string iface;
};


Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string& obj_path,
                                           const std::string& iface);



VoidResult ebpf_set_flow_rate(const EbpfHandle& h,
                               uint64_t flow_key,
                               uint64_t rate_bps);


Result<int> ebpf_load_kill_switch(const std::string& obj_path,
                                   const std::string& iface);


VoidResult ebpf_set_kill_switch(int xdp_map_fd, bool active);


void ebpf_unload_all(const std::string& iface);

#else


struct EbpfHandle { int tc_prog_fd = -1; int map_fd = -1; std::string iface; };

inline Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string&, const std::string&) {
  return Error{"eBPF not supported on this platform"};
}
inline VoidResult ebpf_set_flow_rate(const EbpfHandle&, uint64_t, uint64_t) {
  return ok_void();
}
inline Result<int> ebpf_load_kill_switch(const std::string&, const std::string&) {
  return Error{"eBPF not supported on this platform"};
}
inline VoidResult ebpf_set_kill_switch(int, bool) { return ok_void(); }
inline void       ebpf_unload_all(const std::string&) {}

#endif

}
}
