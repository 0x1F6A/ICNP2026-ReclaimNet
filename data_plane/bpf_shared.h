#pragma once
#ifdef RECLAIMNET_BPF
#define RN_U8 __u8
#define RN_U16 __u16
#define RN_U32 __u32
#define RN_U64 __u64
#else
#include <stdint.h>
#ifdef __linux__
#include <linux/bpf.h>
#else
struct bpf_spin_lock {
  uint32_t val;
};
#endif
#define RN_U8 uint8_t
#define RN_U16 uint16_t
#define RN_U32 uint32_t
#define RN_U64 uint64_t
#endif
#define MAX_FLOWS 65536
#define RECLAIMNET_STATS_INTERVAL_NS 100000000ULL
#define RECLAIMNET_BURST_NS 50000000ULL
#define RN_CLASS_RESEARCH 0
#define RN_CLASS_CHECKPOINT 8
#define RN_CLASS_MIGRATION 34
#define RN_CLASS_EMERGENCY 46
#ifdef RECLAIMNET_BPF
#define RN_INLINE static inline __attribute__((always_inline))
#else
#define RN_INLINE static inline
#endif
struct flow_key {
  RN_U32 src_ip;
  RN_U32 dst_ip;
  RN_U16 src_port;
  RN_U16 dst_port;
  RN_U8 protocol;
  RN_U8 pad[3];
};
struct flow_state {
  struct bpf_spin_lock lock;
  RN_U32 traffic_class;
  RN_U64 rate_bytes_per_sec;
  RN_U64 tokens_bytes;
  RN_U64 last_ts_ns;
  RN_U64 fractional_nano_bytes;
};
struct rn_rate_config {
  RN_U64 rate_bytes_per_sec;
  RN_U32 traffic_class;
  RN_U32 pad;
};
struct flow_counter {
  struct bpf_spin_lock lock;
  RN_U32 pad;
  struct flow_key key;
  RN_U64 bytes_tx;
  RN_U64 bytes_rx;
  RN_U64 packets_tx;
  RN_U64 packets_rx;
  RN_U64 dropped;
  RN_U64 last_ts_ns;
  RN_U64 window_start_ns;
  RN_U64 window_tx;
  RN_U64 window_rx;
  RN_U64 tx_bytes_per_sec;
  RN_U64 rx_bytes_per_sec;
};
struct traffic_ports {
  RN_U16 coordinator_port;
  RN_U16 control_port;
  RN_U16 migration_port;
  RN_U16 pad;
};
struct rn_reclaim_state {
  RN_U32 active;
  RN_U32 scheduled;
  RN_U64 grace_deadline_mono_ns;
  RN_U16 control_port;
  RN_U16 migration_port;
  RN_U32 pad;
};
RN_INLINE RN_U64 rn_flow_hash(const struct flow_key *key) {
  RN_U64 hash = 14695981039346656037ULL;
  const RN_U8 *bytes = (const RN_U8 *)key;
#ifdef RECLAIMNET_BPF
#pragma unroll
#endif
  for (int i = 0; i < 16; ++i) {
    hash ^= bytes[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}
RN_INLINE RN_U64 rn_burst_bytes(RN_U64 rate_bytes, RN_U64 packet_bytes) {
  RN_U64 burst = rate_bytes / 20;
  return burst > packet_bytes ? burst : packet_bytes;
}
RN_INLINE int rn_take_tokens(struct flow_state *state, RN_U64 now_ns,
                             RN_U64 bytes) {
  RN_U64 elapsed = now_ns - state->last_ts_ns;
  state->last_ts_ns = now_ns;
  if (!state->rate_bytes_per_sec)
    return 0;
  RN_U64 rate = state->rate_bytes_per_sec;
  RN_U64 cap = rn_burst_bytes(rate, bytes);
  if (state->tokens_bytes > cap)
    state->tokens_bytes = cap;
  RN_U64 seconds = elapsed / 1000000000ULL;
  if (seconds > cap / rate) {
    state->tokens_bytes = cap;
    state->fractional_nano_bytes = 0;
  } else {
    RN_U64 remainder = elapsed % 1000000000ULL;
    RN_U64 fractional =
        (rate % 1000000000ULL) * remainder + state->fractional_nano_bytes;
    RN_U64 added = seconds * rate + (rate / 1000000000ULL) * remainder +
                   fractional / 1000000000ULL;
    state->fractional_nano_bytes = fractional % 1000000000ULL;
    if (added >= cap - state->tokens_bytes)
      state->tokens_bytes = cap;
    else
      state->tokens_bytes += added;
  }
  if (state->tokens_bytes < bytes)
    return 0;
  state->tokens_bytes -= bytes;
  return 1;
}
RN_INLINE RN_U32 rn_default_class(const struct flow_key *key,
                                  const struct traffic_ports *ports) {
  if (key->src_port == ports->coordinator_port ||
      key->dst_port == ports->coordinator_port ||
      key->src_port == ports->control_port ||
      key->dst_port == ports->control_port)
    return RN_CLASS_EMERGENCY;
  if (key->src_port == ports->migration_port ||
      key->dst_port == ports->migration_port)
    return RN_CLASS_MIGRATION;
  return RN_CLASS_RESEARCH;
}
RN_INLINE int rn_reclaim_drop(const struct rn_reclaim_state *state,
                              RN_U64 now_ns, int tcp, int syn, int ack,
                              RN_U16 src_port, RN_U16 dst_port,
                              int known_migration) {
  if (!state->active)
    return 0;
  if (src_port == 50051 || dst_port == 50051 || src_port == 50052 ||
      dst_port == 50052 ||
      (state->control_port &&
       (src_port == state->control_port || dst_port == state->control_port)))
    return 0;
  if (tcp && syn && !ack)
    return 1;
  if (known_migration)
    return now_ns >= state->grace_deadline_mono_ns;
  return 1;
}
