CLANG ?= clang
BPFTOOL ?= bpftool
ARCH ?= $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')
VMLINUX_BTF ?= /sys/kernel/btf/vmlinux
BPF_CFLAGS := -O2 -g -target bpf -D__TARGET_ARCH_$(ARCH) -I$(CURDIR) -I/usr/include -Wall -Werror -Wno-missing-declarations
SRCS := tc_ratelimit.bpf.c kill_switch.bpf.c flow_telemetry.bpf.c
OBJS := $(SRCS:.c=.o)
.DELETE_ON_ERROR:
.PHONY: all clean
all: $(OBJS)
vmlinux.h:
	$(BPFTOOL) btf dump file $(VMLINUX_BTF) format c > $@
%.bpf.o: %.bpf.c bpf_shared.h packet_parser.h vmlinux.h
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
clean:
	rm -f $(OBJS) vmlinux.h
