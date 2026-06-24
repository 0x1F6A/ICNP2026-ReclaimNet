#include "reclaimnet/ebpf_loader.h"
#include "reclaimnet/time.h"
#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#if defined(__linux__) && RECLAIMNET_HAVE_LIBBPF
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <net/if.h>
#include <unistd.h>
using namespace reclaimnet;
using namespace reclaimnet::agent;
namespace {
uint16_t packet_checksum(const unsigned char *bytes, size_t length) {
  uint32_t sum = 0;
  for (size_t i = 0; i < length; i += 2)
    sum += (uint16_t(bytes[i]) << 8) | bytes[i + 1];
  while (sum >> 16)
    sum = (sum & 65535) + (sum >> 16);
  return static_cast<uint16_t>(~sum);
}
std::vector<unsigned char> packet(uint16_t source, uint16_t destination,
                                  uint8_t flags = 16) {
  std::vector<unsigned char> data(1054);
  data[12] = 8;
  data[14] = 0x45;
  data[15] = 3;
  data[16] = 4;
  data[17] = 16;
  data[22] = 64;
  data[23] = 6;
  data[26] = 10;
  data[29] = 1;
  data[30] = 10;
  data[33] = 2;
  data[34] = source >> 8;
  data[35] = source;
  data[36] = destination >> 8;
  data[37] = destination;
  data[46] = 0x50;
  data[47] = flags;
  uint16_t checksum = packet_checksum(data.data() + 14, 20);
  data[24] = checksum >> 8;
  data[25] = checksum;
  return data;
}
struct PacketResult {
  uint32_t verdict;
  std::vector<unsigned char> data;
};
PacketResult run_packet(int fd, const std::vector<unsigned char> &packet) {
  std::vector<unsigned char> output(2048);
  bpf_test_run_opts options{};
  options.sz = sizeof(options);
  options.data_in = packet.data();
  options.data_size_in = packet.size();
  options.data_out = output.data();
  options.data_size_out = output.size();
  options.repeat = 1;
  if (bpf_prog_test_run_opts(fd, &options))
    throw std::runtime_error(std::strerror(errno));
  output.resize(options.data_size_out);
  return {options.retval, std::move(output)};
}
class BpfLive : public testing::Test {
protected:
  EbpfHandle handle;
  int kill_map = -1;
  int kill_program = -1;
  std::string iface;
  void SetUp() override {
    const char *objects = std::getenv("RECLAIMNET_BPF_TEST_OBJECT_DIR");
    if (!objects)
      GTEST_SKIP();
    const char *interface = std::getenv("RECLAIMNET_BPF_TEST_IFACE");
    iface = interface ? interface : "eth0";
    auto tc = ebpf_load_tc_ratelimit(
        std::string(objects) + "/tc_ratelimit.bpf.o", iface);
    ASSERT_TRUE(tc) << tc.error().message;
    handle = tc.value();
    auto kill = ebpf_load_kill_switch(
        std::string(objects) + "/kill_switch.bpf.o", iface);
    ASSERT_TRUE(kill) << kill.error().message;
    kill_map = kill.value();
    bpf_tc_hook hook{};
    hook.sz = sizeof(hook);
    hook.ifindex = if_nametoindex(iface.c_str());
    hook.attach_point = BPF_TC_INGRESS;
    bpf_tc_opts options{};
    options.sz = sizeof(options);
    options.handle = 3;
    options.priority = 10;
    ASSERT_EQ(bpf_tc_query(&hook, &options), 0);
    kill_program = bpf_prog_get_fd_by_id(options.prog_id);
    ASSERT_GE(kill_program, 0);
  }
  void TearDown() override {
    if (kill_program >= 0)
      close(kill_program);
    ebpf_unload_all(iface);
  }
};
TEST_F(BpfLive, KernelDscpMarkingPreservesEcnAndIpv4Checksum) {
  for (const auto &value : {std::pair<uint16_t, unsigned>{50051, 46},
                            {50052, 46},
                            {7600, 34},
                            {8888, 0}}) {
    auto result = run_packet(handle.tc_prog_fd, packet(20000, value.first));
    ASSERT_EQ(result.verdict, 0u);
    EXPECT_EQ(result.data[15] >> 2, value.second);
    EXPECT_EQ(result.data[15] & 3, 3u);
    EXPECT_EQ(packet_checksum(result.data.data() + 14, 20), 0u);
  }
  FiveTuple tuple{0x0a000001, 0x0a000002, 20000, 7600, 6};
  ASSERT_TRUE(ebpf_set_flow_policy(handle, tuple, 8000000,
                                   TrafficClass::P1_CHECKPOINT));
  auto result = run_packet(handle.tc_prog_fd, packet(20000, 7600));
  EXPECT_EQ(result.data[15] >> 2, 8u);
  EXPECT_EQ(packet_checksum(result.data.data() + 14, 20), 0u);
  auto handshake = run_packet(handle.tc_prog_fd, packet(20000, 7600, 2));
  EXPECT_EQ(handshake.data[15] >> 2, 34u);
  EXPECT_EQ(packet_checksum(handshake.data.data() + 14, 20), 0u);
}
TEST_F(BpfLive, KernelTokenBucketHonorsAssignedRateAndExportsCounters) {
  FiveTuple tuple{0x0a000001, 0x0a000002, 20000, 7600, 6};
  ASSERT_TRUE(ebpf_set_flow_policy(handle, tuple, 80000,
                                   TrafficClass::PLANNED_MIGRATION));
  auto data = packet(20000, 7600);
  EXPECT_EQ(run_packet(handle.tc_prog_fd, data).verdict, 0u);
  ASSERT_TRUE(ebpf_set_flow_policy(handle, tuple, 80000,
                                   TrafficClass::PLANNED_MIGRATION));
  EXPECT_EQ(run_packet(handle.tc_prog_fd, data).verdict, 2u);
  std::this_thread::sleep_for(std::chrono::milliseconds(110));
  EXPECT_EQ(run_packet(handle.tc_prog_fd, data).verdict, 0u);
  auto counters = ebpf_read_flow_stats(handle);
  ASSERT_TRUE(counters) << counters.error().message;
  auto target =
      std::find_if(counters.value().begin(), counters.value().end(),
                   [&](const auto &counter) { return counter.key == tuple; });
  ASSERT_NE(target, counters.value().end());
  EXPECT_EQ(target->bytes_tx, data.size() * 2);
  EXPECT_EQ(target->dropped, 1u);
  ASSERT_TRUE(ebpf_remove_flow(handle, tuple));
  EXPECT_EQ(run_packet(handle.tc_prog_fd, data).verdict, 0u);
}
TEST_F(BpfLive, KernelKillSwitchRejectsSynAndZeroGraceButAllowsControl) {
  FiveTuple tuple{0x0a000001, 0x0a000002, 20000, 7600, 6};
  ASSERT_TRUE(ebpf_set_flow_policy(handle, tuple, 8000000,
                                   TrafficClass::PLANNED_MIGRATION));
  ASSERT_TRUE(
      ebpf_set_reclaim_policy(kill_map, true, true, mono_ns() + 1000000000));
  EXPECT_EQ(run_packet(kill_program, packet(20000, 7600, 2)).verdict, 2u);
  EXPECT_EQ(run_packet(kill_program, packet(20000, 7600, 16)).verdict, 0u);
  EXPECT_EQ(run_packet(kill_program, packet(20000, 8888, 16)).verdict, 2u);
  EXPECT_EQ(run_packet(kill_program, packet(20000, 50052, 2)).verdict, 0u);
  FiveTuple outbound{0x0a000002, 0x0a000001, 21000, 7600, 6};
  ASSERT_TRUE(ebpf_set_flow_policy(handle, outbound, 8000000,
                                   TrafficClass::PLANNED_MIGRATION));
  EXPECT_EQ(run_packet(kill_program, packet(7600, 21000, 18)).verdict, 0u);
  EXPECT_EQ(run_packet(kill_program, packet(7600, 21000, 2)).verdict, 2u);
  ASSERT_TRUE(ebpf_set_reclaim_policy(kill_map, true, false, 0));
  EXPECT_EQ(run_packet(kill_program, packet(20000, 7600, 16)).verdict, 2u);
  EXPECT_EQ(run_packet(kill_program, packet(20000, 50051, 2)).verdict, 0u);
  EXPECT_EQ(run_packet(kill_program, packet(7600, 21000, 18)).verdict, 2u);
}
TEST_F(BpfLive, UnloadDetachesBothTcDirections) {
  ebpf_unload_all(iface);
  bpf_tc_hook hook{};
  hook.sz = sizeof(hook);
  hook.ifindex = if_nametoindex(iface.c_str());
  bpf_tc_opts options{};
  options.sz = sizeof(options);
  options.handle = 1;
  options.priority = 20;
  hook.attach_point = BPF_TC_EGRESS;
  EXPECT_NE(bpf_tc_query(&hook, &options), 0);
  options.handle = 3;
  options.priority = 10;
  hook.attach_point = BPF_TC_INGRESS;
  EXPECT_NE(bpf_tc_query(&hook, &options), 0);
}
}
#endif
