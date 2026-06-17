#include "reclaimnet/time.h"
#include "reclaimnet/topology.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
using namespace reclaimnet;
using namespace reclaimnet::control;

TEST(Topology, MultiHopBottleneckAndCanonicalLevels) {
  TopologyGraph graph;
  graph.add_link({"A", "rack", 1e9, 1, TopologyLevel::INTRA_RACK});
  graph.add_link({"rack", "B", 1e8, 2, TopologyLevel::INTRA_BUILDING});
  auto path = graph.shortest_path("A", "B");
  ASSERT_TRUE(path);
  EXPECT_EQ(path->path, (std::vector<NodeId>{"A", "rack", "B"}));
  EXPECT_DOUBLE_EQ(path->bottleneck_bps, 1e8);
  EXPECT_EQ(path->max_level, TopologyLevel::INTRA_BUILDING);
  EXPECT_FALSE(graph.shortest_path("A", "missing"));
}
TEST(Topology, DirectedLinkDoesNotCreateReversePath) {
  TopologyGraph graph;
  graph.add_link({"A", "B", 1e9, 1, TopologyLevel::INTRA_RACK}, false);
  EXPECT_TRUE(graph.shortest_path("A", "B"));
  EXPECT_FALSE(graph.shortest_path("B", "A"));
}
TEST(Topology, ReadsActualTraceV3Topology) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON disabled";
#else
  auto graph = TopologyGraph::load_from_file(
      std::string(RECLAIMNET_TEST_FIXTURE_DIR) + "/trace_1/topology.json");
  ASSERT_TRUE(graph) << graph.error().message;
  EXPECT_GE(graph.value().node_count(), 53u);
  auto path = graph.value().shortest_path("node-020", "node-011");
  ASSERT_TRUE(path);
  EXPECT_EQ(path->max_level, TopologyLevel::INTRA_BUILDING);
  EXPECT_GT(path->bottleneck_bps, 0);
#endif
}
TEST(Topology, UnknownLevelIsAnError) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON disabled";
#else
  auto file =
      std::filesystem::temp_directory_path() /
      ("reclaimnet-invalid-topology-" + std::to_string(mono_ns()) + ".json");
  {
    std::ofstream stream(file);
    stream << R"({"links":[{"src":"A","dst":"B","level":"unknown"}]})";
  }
  EXPECT_FALSE(TopologyGraph::load_from_file(file.string()));
  std::filesystem::remove(file);
#endif
}
