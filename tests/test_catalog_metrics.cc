#include "reclaimnet/checkpoint_catalog.h"
#include "reclaimnet/metrics_server.h"
#include "reclaimnet/prometheus.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include "reclaimnet/trace.h"
#include "reclaimnet/trace_writer.h"
#include <arpa/inet.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
using namespace reclaimnet;
using namespace reclaimnet::control;
TEST(CheckpointCatalog, SelectsNewestCompleteSurvivingGangBeforeDecision) {
  CheckpointCatalog catalog;
  ReplicaCheckpoint replica;
  replica.job_id = "job";
  replica.checkpoint_id = "checkpoint-1";
  replica.sequence = 1;
  replica.completed_at_ns = 100;
  replica.size_bytes = 4;
  replica.path = "/state/checkpoint-1";
  replica.checksum_sha256 = std::string(64, 'a');
  replica.gang_size = 2;
  replica.source_node_ids = {"source-0", "source-1"};
  replica.replica_node_id = "dest-0";
  replica.rank = 0;
  ASSERT_TRUE(catalog.commit(replica));
  EXPECT_TRUE(
      catalog.freshest_surviving("job", {"dest-0", "dest-1"}, {}, 200).empty());
  replica.rank = 1;
  replica.replica_node_id = "dest-1";
  ASSERT_TRUE(catalog.commit(replica));
  auto selected =
      catalog.freshest_surviving("job", {"dest-0", "dest-1"}, {}, 200);
  ASSERT_EQ(selected.size(), 2u);
  EXPECT_EQ(selected[0].sequence, 1u);
  replica.sequence = 2;
  replica.checkpoint_id = "checkpoint-2";
  replica.completed_at_ns = 300;
  ASSERT_TRUE(catalog.commit(replica));
  selected = catalog.freshest_surviving("job", {"dest-0", "dest-1"}, {}, 400);
  ASSERT_EQ(selected.size(), 2u);
  EXPECT_EQ(selected[0].sequence, 1u);
  replica.rank = 0;
  replica.replica_node_id = "dest-0";
  ASSERT_TRUE(catalog.commit(replica));
  selected = catalog.freshest_surviving("job", {"dest-0", "dest-1"}, {}, 400);
  ASSERT_EQ(selected.size(), 2u);
  EXPECT_EQ(selected[0].sequence, 2u);
  catalog.invalidate_node("dest-0", 350);
  EXPECT_TRUE(
      catalog.freshest_surviving("job", {"dest-0", "dest-1"}, {}, 400).empty());
}
TEST(MetricsServer, ServesCurrentRegistryThroughHttp) {
  MetricRegistry::instance()
      .gauge("reclaimnet_test_value", "Test value", {})
      .set(17);
  MetricsServer server;
  ASSERT_TRUE(server.start(0));
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(server.port());
  ASSERT_EQ(
      connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)), 0);
  std::string request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
  ASSERT_EQ(send(fd, request.data(), request.size(), 0),
            static_cast<ssize_t>(request.size()));
  std::string reply;
  char buffer[4096];
  ssize_t size;
  while ((size = recv(fd, buffer, sizeof(buffer), 0)) > 0)
    reply.append(buffer, size);
  close(fd);
  server.stop();
  EXPECT_NE(reply.find("HTTP/1.1 200 OK"), std::string::npos);
  EXPECT_NE(reply.find("reclaimnet_test_value 17"), std::string::npos);
}
#if RECLAIMNET_HAVE_JSON
TEST(TraceWriter, WritesSchemaAndManifestWithActualFileIntegrity) {
  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("reclaimnet-writer-" + std::to_string(now_ns()));
  trace::Writer writer(path.string(), "runtime-test");
  ASSERT_TRUE(writer.initialize(.2, 18000, 7000000000, .3, 50000000));
  ASSERT_TRUE(writer.append(
      "reclaims.jsonl",
      R"({"node_id":"node-1","type":"EMERGENCY","tau_seconds":0.0,"timestamp":1772302390292933888})"));
  ASSERT_TRUE(writer.finalize());
  nlohmann::json manifest;
  std::ifstream(path / "manifest.json") >> manifest;
  EXPECT_EQ(manifest["counts"]["reclaims"], 1);
  EXPECT_EQ(manifest["files"]["reclaims.jsonl"]["bytes"],
            fs::file_size(path / "reclaims.jsonl"));
  EXPECT_EQ(manifest["files"]["reclaims.jsonl"]["sha256"],
            sha256_file((path / "reclaims.jsonl").string()).value());
  std::ifstream schema(path / "schema.json");
  std::string text;
  std::getline(schema, text);
  auto decoded = trace::Parse<trace::Schema>(text);
  EXPECT_TRUE(decoded);
  fs::remove_all(path);
}
#endif
