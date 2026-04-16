






#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#include "bpf_shared.h"




struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FLOWS);
  __type(key,   __u64);
  __type(value, struct flow_state);
} flow_rates SEC(".maps");


struct {
  __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
  __uint(key_size,   sizeof(__u32));
  __uint(value_size, sizeof(__u32));
} flow_events SEC(".maps");



static __always_inline __u64 flow_hash(struct flow_key *k) {

  __u64 h = 14695981039346656037ULL;
  __u8 *p = (__u8 *)k;
  #pragma unroll
  for (int i = 0; i < (int)sizeof(*k); i++) {
    h ^= (__u64)p[i];
    h *= 1099511628211ULL;
  }
  return h;
}

static __always_inline int parse_key(struct __sk_buff *skb, struct flow_key *k) {
  void *data     = (void *)(long)skb->data;
  void *data_end = (void *)(long)skb->data_end;

  struct ethhdr *eth = data;
  if ((void *)(eth + 1) > data_end) return -1;
  if (bpf_ntohs(eth->h_proto) != ETH_P_IP) return -1;

  struct iphdr *iph = (struct iphdr *)(eth + 1);
  if ((void *)(iph + 1) > data_end) return -1;

  k->src_ip   = iph->saddr;
  k->dst_ip   = iph->daddr;
  k->protocol = iph->protocol;

  if (iph->protocol == IPPROTO_TCP) {
    struct tcphdr *tcp = (struct tcphdr *)((void *)iph + iph->ihl * 4);
    if ((void *)(tcp + 1) > data_end) return -1;
    k->src_port = bpf_ntohs(tcp->source);
    k->dst_port = bpf_ntohs(tcp->dest);
  } else if (iph->protocol == IPPROTO_UDP) {
    struct udphdr *udp = (struct udphdr *)((void *)iph + iph->ihl * 4);
    if ((void *)(udp + 1) > data_end) return -1;
    k->src_port = bpf_ntohs(udp->source);
    k->dst_port = bpf_ntohs(udp->dest);
  } else {
    k->src_port = 0;
    k->dst_port = 0;
  }
  return 0;
}



SEC("tc")
int tc_ratelimit(struct __sk_buff *skb) {
  struct flow_key k = {};
  if (parse_key(skb, &k) < 0)
    return TC_ACT_OK;

  __u64 key_hash = flow_hash(&k);
  struct flow_state *st = bpf_map_lookup_elem(&flow_rates, &key_hash);
  if (!st || st->rate_bps == 0)
    return TC_ACT_OK;

  __u64 now_ns  = bpf_ktime_get_ns();
  __u64 elapsed = now_ns - st->last_ts_ns;


  __u64 new_tokens = (elapsed * st->rate_bps) / 1000000000ULL;
  st->tokens      += new_tokens;

  __u64 max_tokens = st->rate_bps / 10;
  if (st->tokens > max_tokens) st->tokens = max_tokens;
  st->last_ts_ns = now_ns;

  __u64 pkt_bits = (__u64)skb->len * 8;
  if (st->tokens >= pkt_bits) {
    st->tokens -= pkt_bits;
    return TC_ACT_OK;
  }


  struct flow_event ev = {};
  ev.key     = k;
  ev.bytes   = skb->len;
  ev.ts_ns   = now_ns;
  ev.dropped = 1;
  bpf_perf_event_output(skb, &flow_events, BPF_F_CURRENT_CPU, &ev, sizeof(ev));

  return TC_ACT_SHOT;
}

char LICENSE[] SEC("license") = "GPL";
