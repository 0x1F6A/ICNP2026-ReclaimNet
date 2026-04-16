



#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include "bpf_shared.h"



struct {
  __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
  __uint(key_size,   sizeof(__u32));
  __uint(value_size, sizeof(__u32));
} telem_events SEC(".maps");



static __always_inline int parse_five_tuple(struct xdp_md *ctx,
                                             struct flow_key *k) {
  void *data     = (void *)(long)ctx->data;
  void *data_end = (void *)(long)ctx->data_end;

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
  }
  return 0;
}



SEC("xdp")
int flow_telemetry(struct xdp_md *ctx) {
  struct flow_key k = {};
  if (parse_five_tuple(ctx, &k) < 0)
    return XDP_PASS;

  __u32 pkt_len = ctx->data_end - ctx->data;

  struct flow_event ev = {};
  ev.key     = k;
  ev.bytes   = pkt_len;
  ev.ts_ns   = bpf_ktime_get_ns();
  ev.dropped = 0;

  bpf_perf_event_output(ctx, &telem_events, BPF_F_CURRENT_CPU, &ev, sizeof(ev));
  return XDP_PASS;
}

char LICENSE[] SEC("license") = "GPL";
