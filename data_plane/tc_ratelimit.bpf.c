#include "vmlinux.h"
#define RECLAIMNET_BPF 1
#include "packet_parser.h"
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key, __u64);
  __type(value, struct rn_rate_config);
} flow_rates SEC(".maps");
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key, __u64);
  __type(value, struct flow_state);
} flow_buckets SEC(".maps");
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
  __type(value, struct flow_counter);
} flow_counters SEC(".maps");
struct {
  __uint(type, BPF_MAP_TYPE_ARRAY);
  __uint(max_entries, 1);
  __type(key, __u32);
  __type(value, struct traffic_ports);
} traffic_ports_map SEC(".maps");
static __always_inline void record(struct __sk_buff *skb,
                                   const struct flow_key *key, int ingress,
                                   int dropped) {
  __u64 hash = rn_flow_hash(key);
  struct flow_counter *counter = bpf_map_lookup_elem(&flow_counters, &hash);
  if (!counter) {
    struct flow_counter initial = {};
    initial.key = *key;
    bpf_map_update_elem(&flow_counters, &hash, &initial, BPF_NOEXIST);
    counter = bpf_map_lookup_elem(&flow_counters, &hash);
    if (!counter)
      return;
  }
  __u64 now = bpf_ktime_get_ns();
  __u32 length = skb->len;
  bpf_spin_lock(&counter->lock);
  if (!counter->window_start_ns)
    counter->window_start_ns = now;
  if (ingress) {
    counter->bytes_rx += length;
    ++counter->packets_rx;
    counter->window_rx += length;
  } else if (!dropped) {
    counter->bytes_tx += length;
    ++counter->packets_tx;
    counter->window_tx += length;
  }
  counter->dropped += dropped;
  counter->last_ts_ns = now;
  __u64 elapsed = now - counter->window_start_ns;
  if (elapsed >= RECLAIMNET_STATS_INTERVAL_NS) {
    counter->tx_bytes_per_sec = counter->window_tx * 1000000000ULL / elapsed;
    counter->rx_bytes_per_sec = counter->window_rx * 1000000000ULL / elapsed;
    counter->window_tx = 0;
    counter->window_rx = 0;
    counter->window_start_ns = now;
  }
  bpf_spin_unlock(&counter->lock);
}
SEC("tc")
int tc_ratelimit(struct __sk_buff *skb) {
  struct rn_packet packet = {};
  if (rn_parse(skb, &packet))
    return RN_TC_OK;
  __u64 hash = rn_flow_hash(&packet.key);
  __u32 *classification = bpf_map_lookup_elem(&flow_classes, &hash);
  struct rn_rate_config *config = bpf_map_lookup_elem(&flow_rates, &hash);
  __u32 zero = 0;
  struct traffic_ports *ports = bpf_map_lookup_elem(&traffic_ports_map, &zero);
  __u8 dscp = config           ? config->traffic_class
              : classification ? *classification
              : ports          ? rn_default_class(&packet.key, ports)
                               : RN_CLASS_RESEARCH;
  if (ports && packet.key.protocol == RN_TCP && (packet.tcp_flags & 2) &&
      (packet.key.src_port == ports->migration_port ||
       packet.key.dst_port == ports->migration_port))
    dscp = RN_CLASS_MIGRATION;
  rn_mark(skb, &packet, dscp);
  int allowed = 1;
  if (config && dscp != RN_CLASS_EMERGENCY) {
    __u64 now = bpf_ktime_get_ns();
    __u32 length = skb->len;
    __u64 rate = config->rate_bytes_per_sec;
    struct flow_state *state = bpf_map_lookup_elem(&flow_buckets, &hash);
    if (!state) {
      struct flow_state initial = {};
      initial.rate_bytes_per_sec = rate;
      initial.tokens_bytes = rn_burst_bytes(rate, length);
      initial.last_ts_ns = now;
      bpf_map_update_elem(&flow_buckets, &hash, &initial, BPF_NOEXIST);
      state = bpf_map_lookup_elem(&flow_buckets, &hash);
    }
    if (!state)
      allowed = 0;
    else {
      bpf_spin_lock(&state->lock);
      state->rate_bytes_per_sec = rate;
      allowed = rn_take_tokens(state, now, length);
      bpf_spin_unlock(&state->lock);
    }
  }
  record(skb, &packet.key, 0, !allowed);
  return allowed ? RN_TC_OK : RN_TC_DROP;
}
SEC("tc")
int tc_flow_ingress(struct __sk_buff *skb) {
  struct rn_packet packet = {};
  if (!rn_parse(skb, &packet))
    record(skb, &packet.key, 1, 0);
  return RN_TC_OK;
}
char LICENSE[] SEC("license") = "GPL";
