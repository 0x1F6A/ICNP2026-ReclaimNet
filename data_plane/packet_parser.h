#pragma once
#include "bpf_shared.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>
#define RN_ETH_IP 0x0800
#define RN_TCP 6
#define RN_UDP 17
#define RN_TC_OK 0
#define RN_TC_DROP 2
struct rn_packet {
  struct flow_key key;
  __u8 tos;
  __u8 tcp_flags;
  __u16 ip_offset;
  __u16 checksum;
};
static __always_inline int rn_parse(struct __sk_buff *skb,
                                    struct rn_packet *packet) {
  struct ethhdr eth;
  struct iphdr ip;
  if (bpf_skb_load_bytes(skb, 0, &eth, sizeof(eth)) ||
      bpf_ntohs(eth.h_proto) != RN_ETH_IP)
    return -1;
  if (bpf_skb_load_bytes(skb, sizeof(eth), &ip, sizeof(ip)) || ip.ihl < 5)
    return -1;
  packet->ip_offset = sizeof(eth);
  packet->key.src_ip = bpf_ntohl(ip.saddr);
  packet->key.dst_ip = bpf_ntohl(ip.daddr);
  packet->key.protocol = ip.protocol;
  packet->tos = ip.tos;
  packet->checksum = ip.check;
  __u32 offset = sizeof(eth) + ip.ihl * 4;
  if (ip.protocol == RN_TCP) {
    struct tcphdr tcp;
    if (bpf_skb_load_bytes(skb, offset, &tcp, sizeof(tcp)))
      return -1;
    packet->key.src_port = bpf_ntohs(tcp.source);
    packet->key.dst_port = bpf_ntohs(tcp.dest);
    packet->tcp_flags = ((__u8 *)&tcp)[13];
  } else if (ip.protocol == RN_UDP) {
    struct udphdr udp;
    if (bpf_skb_load_bytes(skb, offset, &udp, sizeof(udp)))
      return -1;
    packet->key.src_port = bpf_ntohs(udp.source);
    packet->key.dst_port = bpf_ntohs(udp.dest);
  }
  return 0;
}
static __always_inline void rn_mark(struct __sk_buff *skb,
                                    const struct rn_packet *packet, __u8 dscp) {
  __u8 tos = (dscp << 2) | (packet->tos & 3);
  if (tos == packet->tos)
    return;
  __u32 sum =
      (~bpf_ntohs(packet->checksum) & 0xffff) + (~packet->tos & 0xffff) + tos;
  sum = (sum & 0xffff) + (sum >> 16);
  sum = (sum & 0xffff) + (sum >> 16);
  __u16 checksum = bpf_htons(~sum);
  bpf_skb_store_bytes(
      skb, packet->ip_offset + bpf_core_field_offset(struct iphdr, tos), &tos,
      sizeof(tos), 0);
  bpf_skb_store_bytes(
      skb, packet->ip_offset + bpf_core_field_offset(struct iphdr, check),
      &checksum, sizeof(checksum), 0);
}
