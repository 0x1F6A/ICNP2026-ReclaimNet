#include "vmlinux.h"
#define RECLAIMNET_BPF 1
#include "packet_parser.h"
struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, 1);
  __type(key, __u32);
  __type(value, struct rn_reclaim_state);
} kill_switch_map SEC(".maps");
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key, __u64);
  __type(value, __u32);
} flow_classes SEC(".maps");
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key, __u64);
  __type(value, __u8);
} migration_connections SEC(".maps");
SEC("tc")
int kill_switch(struct __sk_buff *skb) {
  __u32 zero = 0;
  struct rn_reclaim_state *state = bpf_map_lookup_elem(&kill_switch_map, &zero);
  if (!state)
    return RN_TC_OK;
  struct rn_packet packet = {};
  if (rn_parse(skb, &packet))
    return RN_TC_OK;
  __u64 hash = rn_flow_hash(&packet.key);
  __u32 *classification = bpf_map_lookup_elem(&flow_classes, &hash);
  struct flow_key reverse = packet.key;
  reverse.src_ip = packet.key.dst_ip;
  reverse.dst_ip = packet.key.src_ip;
  reverse.src_port = packet.key.dst_port;
  reverse.dst_port = packet.key.src_port;
  __u64 reverse_hash = rn_flow_hash(&reverse);
  if (!classification)
    classification = bpf_map_lookup_elem(&flow_classes, &reverse_hash);
  int migration_port = packet.key.src_port == state->migration_port ||
                       packet.key.dst_port == state->migration_port;
  if (!state->active && packet.key.protocol == RN_TCP && migration_port) {
    __u8 value = 1;
    bpf_map_update_elem(&migration_connections, &hash, &value, BPF_ANY);
  }
  int known = (classification && *classification == RN_CLASS_MIGRATION) ||
              bpf_map_lookup_elem(&migration_connections, &hash) != 0;
  int drop =
      rn_reclaim_drop(state, bpf_ktime_get_ns(), packet.key.protocol == RN_TCP,
                      packet.tcp_flags & 2, packet.tcp_flags & 16,
                      packet.key.src_port, packet.key.dst_port, known);
  return drop ? RN_TC_DROP : RN_TC_OK;
}
char LICENSE[] SEC("license") = "GPL";
