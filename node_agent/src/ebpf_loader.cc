


#include "reclaimnet/ebpf_loader.h"
#include "reclaimnet/log.h"

#ifdef __linux__


#if __has_include(<bpf/libbpf.h>)
#  include <bpf/libbpf.h>
#  include <bpf/bpf.h>
#  define HAVE_LIBBPF 1
#else
#  define HAVE_LIBBPF 0
#endif

#if __has_include(<linux/bpf.h>)
#  include <linux/if_link.h>
#endif

#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <string>

namespace reclaimnet {
namespace agent {



Result<EbpfHandle> ebpf_load_tc_ratelimit(const std::string& obj_path,
                                           const std::string& iface) {
#if HAVE_LIBBPF
  struct bpf_object* obj = bpf_object__open(obj_path.c_str());
  if (!obj) return Error{"bpf_object__open failed for " + obj_path};

  if (bpf_object__load(obj)) {
    bpf_object__close(obj);
    return Error{"bpf_object__load failed"};
  }

  struct bpf_program* prog = bpf_object__find_program_by_name(obj, "tc_ratelimit");
  if (!prog) {
    bpf_object__close(obj);
    return Error{"Program 'tc_ratelimit' not found in " + obj_path};
  }

  int prog_fd = bpf_program__fd(prog);
  int map_fd  = bpf_object__find_map_fd_by_name(obj, "flow_rates");

  EbpfHandle h;
  h.tc_prog_fd = prog_fd;
  h.map_fd     = map_fd;
  h.iface      = iface;


  LOG_INFO("eBPF: loaded tc_ratelimit on iface=%s prog_fd=%d map_fd=%d",
           iface.c_str(), prog_fd, map_fd);
  return h;
#else
  (void)obj_path; (void)iface;
  return Error{"libbpf not available; cannot load eBPF programs"};
#endif
}



VoidResult ebpf_set_flow_rate(const EbpfHandle& h,
                               uint64_t flow_key,
                               uint64_t rate_bps) {
#if HAVE_LIBBPF
  if (h.map_fd < 0) return err_void("Invalid eBPF map fd");
  if (bpf_map_update_elem(h.map_fd, &flow_key, &rate_bps, BPF_ANY) < 0) {
    return Error{std::string("bpf_map_update_elem failed: ") + strerror(errno)};
  }
  return ok_void();
#else
  (void)h; (void)flow_key; (void)rate_bps;
  return ok_void();
#endif
}



Result<int> ebpf_load_kill_switch(const std::string& obj_path,
                                   const std::string& iface) {
#if HAVE_LIBBPF
  struct bpf_object* obj = bpf_object__open(obj_path.c_str());
  if (!obj) return Error{"bpf_object__open failed for " + obj_path};
  if (bpf_object__load(obj)) {
    bpf_object__close(obj);
    return Error{"bpf_object__load failed"};
  }
  int map_fd = bpf_object__find_map_fd_by_name(obj, "kill_switch_map");
  LOG_INFO("eBPF: loaded kill_switch on iface=%s map_fd=%d", iface.c_str(), map_fd);
  return map_fd;
#else
  (void)obj_path; (void)iface;
  return Error{"libbpf not available"};
#endif
}



VoidResult ebpf_set_kill_switch(int map_fd, bool active) {
#if HAVE_LIBBPF
  if (map_fd < 0) return err_void("Invalid kill-switch map fd");
  uint32_t key = 0;
  uint32_t val = active ? 1u : 0u;
  if (bpf_map_update_elem(map_fd, &key, &val, BPF_ANY) < 0) {
    return Error{std::string("kill_switch update failed: ") + strerror(errno)};
  }
  LOG_INFO("eBPF: kill_switch %s", active ? "ACTIVATED" : "deactivated");
  return ok_void();
#else
  (void)map_fd; (void)active;
  return ok_void();
#endif
}



void ebpf_unload_all(const std::string& iface) {
  LOG_INFO("eBPF: unloading programs for iface=%s", iface.c_str());


  (void)iface;
}

}
}

#endif
