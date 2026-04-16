



CLANG   ?= clang
LLC     ?= llc
ARCH    ?= $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')
BPF_CFLAGS := -O2 -g -target bpf -D__TARGET_ARCH_$(ARCH) \
              -I/usr/include -I/usr/local/include \
              -I$(CURDIR) \
              -Wall -Wno-unused-value -Wno-pointer-sign

SRCS := tc_ratelimit.bpf.c kill_switch.bpf.c flow_telemetry.bpf.c
OBJS := $(SRCS:.c=.o)

.PHONY: all clean

all: $(OBJS)

%.bpf.o: %.bpf.c bpf_shared.h
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS)
