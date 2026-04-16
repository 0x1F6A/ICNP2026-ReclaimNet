



#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include "bpf_shared.h"



struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, KILL_SWITCH_MAP_SIZE);
  __type(key,   __u32);
  __type(value, __u32);
} kill_switch_map SEC(".maps");



SEC("xdp")
int kill_switch(struct xdp_md *ctx) {
  __u32 key   = 0;
  __u32 *active = bpf_map_lookup_elem(&kill_switch_map, &key);
  if (active && *active) {
    return XDP_DROP;
  }
  return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
