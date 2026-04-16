



#pragma once

#include <stdint.h>


#define RECLAIMNET_MAGIC 0x524E4D47u


#define MAX_FLOWS 65536


#define KILL_SWITCH_MAP_SIZE 1



struct flow_key {
  uint32_t src_ip;
  uint32_t dst_ip;
  uint16_t src_port;
  uint16_t dst_port;
  uint8_t  protocol;
  uint8_t  _pad[3];
} __attribute__((packed));



struct flow_state {
  uint64_t rate_bps;
  uint64_t tokens;
  uint64_t last_ts_ns;
};



struct flow_event {
  struct flow_key key;
  uint64_t        bytes;
  uint64_t        ts_ns;
  uint8_t         dropped;
  uint8_t         _pad[7];
};
