#include "vmlinux.h"
#define RECLAIMNET_BPF 1
#include "packet_parser.h"
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key, __u64);
  __type(value, struct flow_counter);
} flow_counters SEC(".maps");
SEC("tc")
int flow_telemetry(struct __sk_buff *skb) {
  struct rn_packet packet = {};
  if (rn_parse(skb, &packet))
    return RN_TC_OK;
  __u64 hash = rn_flow_hash(&packet.key);
  struct flow_counter *counter = bpf_map_lookup_elem(&flow_counters, &hash);
  if (!counter) {
    struct flow_counter initial = {};
    initial.key = packet.key;
    bpf_map_update_elem(&flow_counters, &hash, &initial, BPF_NOEXIST);
    counter = bpf_map_lookup_elem(&flow_counters, &hash);
    if (!counter)
      return RN_TC_OK;
  }
  __u64 now = bpf_ktime_get_ns();
  __u32 bytes = skb->len;
  bpf_spin_lock(&counter->lock);
  counter->bytes_rx += bytes;
  ++counter->packets_rx;
  counter->last_ts_ns = now;
  if (!counter->window_start_ns)
    counter->window_start_ns = now;
  counter->window_rx += bytes;
  __u64 elapsed = now - counter->window_start_ns;
  if (elapsed >= RECLAIMNET_STATS_INTERVAL_NS) {
    counter->rx_bytes_per_sec = counter->window_rx * 1000000000ULL / elapsed;
    counter->window_rx = 0;
    counter->window_start_ns = now;
  }
  bpf_spin_unlock(&counter->lock);
  return RN_TC_OK;
}
char LICENSE[] SEC("license") = "GPL";
