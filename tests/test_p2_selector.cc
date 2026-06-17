

#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/p2_selector.h"
#include "reclaimnet/time.h"
#include "reclaimnet/topology.h"
#include <gtest/gtest.h>

using namespace reclaimnet;
using namespace reclaimnet::control;

class P2Test : public ::testing::Test {
protected:
  void SetUp() override {

    Link lk1{"node-src", "node-dst1", 100e9, 2.0, TopologyLevel::INTRA_RACK};
    Link lk2{"node-src", "node-dst2", 40e9, 10.0,
             TopologyLevel::CROSS_BUILDING};
    topo.add_link(lk1);
    topo.add_link(lk2);

    NodeRecord r1{};
    r1.node_id = "node-dst1";
    r1.state = NodeState::ONLINE;
    r1.link_capacity_bps = (BandwidthBps)100e9;
    r1.telemetry.vram_free_bytes = 16ULL * 1024 * 1024 * 1024;
    r1.telemetry.updated_ns = now_ns();
    nodes.upsert(r1);

    NodeRecord r2{};
    r2.node_id = "node-dst2";
    r2.state = NodeState::ONLINE;
    r2.link_capacity_bps = (BandwidthBps)40e9;
    r2.telemetry.vram_free_bytes = 16ULL * 1024 * 1024 * 1024;
    r2.telemetry.updated_ns = now_ns();
    nodes.upsert(r2);
  }

  TopologyGraph topo;
  HazardEstimator hazard;
  NodeRegistry nodes;
};

TEST_F(P2Test, SelectsBestDestination) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id = "job-1";
  q.source_node = "node-src";
  q.checkpoint_size_b = 1000000000ULL;
  q.reclaim_deadline_s = 300.0;
  q.T_restore_s = 5.0;

  auto r = sel.select(q);
  EXPECT_TRUE(r.success);
  EXPECT_FALSE(r.selected.empty());
  EXPECT_GE(r.ranked.size(), 2u);
}

TEST_F(P2Test, InfeasibleTightDeadline) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id = "job-tight";
  q.source_node = "node-src";
  q.checkpoint_size_b = 400000000000ULL;
  q.reclaim_deadline_s = 0.1;
  q.T_restore_s = 0.0;

  auto r = sel.select(q);
  EXPECT_FALSE(r.success);
}

TEST(P2Selector, TopologyDistScore) {
  EXPECT_EQ(P2Selector::topology_dist_score(TopologyLevel::INTRA_RACK), 0.0);
  EXPECT_EQ(P2Selector::topology_dist_score(TopologyLevel::CORE_CROSSING), 1.0);
  EXPECT_GT(P2Selector::topology_dist_score(TopologyLevel::CROSS_BUILDING),
            P2Selector::topology_dist_score(TopologyLevel::INTRA_RACK));
}

TEST_F(P2Test, RankingBestFirst) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id = "job-rank";
  q.source_node = "node-src";
  q.checkpoint_size_b = 500000000ULL;
  q.reclaim_deadline_s = 200.0;
  q.T_restore_s = 1.0;

  auto r = sel.select(q);
  ASSERT_GE(r.ranked.size(), 2u);

  for (size_t i = 0; i + 1 < r.ranked.size(); ++i) {
    if (r.ranked[i].feasible && r.ranked[i + 1].feasible) {
      EXPECT_LE(r.ranked[i].composite_cost, r.ranked[i + 1].composite_cost);
    }
  }
}

namespace {
NodeRecord protocol_node(const std::string &id, const std::string &building,
                         uint64_t vram = 1024, uint32_t gpu = 1,
                         double capability = 8.6) {
  NodeRecord node;
  node.node_id = id;
  node.building = building;
  node.state = NodeState::ONLINE;
  node.link_capacity_bps = 1000000000;
  node.telemetry.vram_free_bytes = vram;
  node.gpu_count = gpu;
  node.gpu_free_count = gpu;
  node.cuda_capability = capability;
  return node;
}
P2Query protocol_query() {
  P2Query query{};
  query.job_id = "job";
  query.source_node = "source";
  query.checkpoint_size_b = 100;
  query.reclaim_deadline_s = 100;
  query.remaining_runtime_s = 100;
  query.decision_ns = 1772298378404020960LL;
  return query;
}
}

TEST(P2Protocol, TraceV3FirstCandidateScore) {
  TopologyGraph topology;
  for (const auto &link : std::vector<Link>{
           {"node-020", "sw-B-r2", 462894860, 3, TopologyLevel::INTRA_RACK},
           {"sw-B-r2", "sw-B-core", 10000000000, 3,
            TopologyLevel::INTRA_BUILDING},
           {"sw-B-core", "sw-core", 442081203, 3,
            TopologyLevel::CROSS_BUILDING},
           {"sw-core", "sw-A-core", 442081203, 3,
            TopologyLevel::CROSS_BUILDING},
           {"sw-A-core", "sw-A-r1", 10000000000, 3,
            TopologyLevel::INTRA_BUILDING},
           {"sw-A-r1", "node-001", 999553417, 3, TopologyLevel::INTRA_RACK}})
    topology.add_link(link);
  HazardEstimator hazard;
  NodeRegistry nodes;
  nodes.upsert(protocol_node("node-020", "BuildingB", 25769803776ULL));
  auto destination = protocol_node("node-001", "BuildingA", 25626905556ULL);
  destination.telemetry.gpu_util_pct = 2;
  destination.historical_migration_cost_s = 37.96862660920308;
  nodes.upsert(destination);
  auto query = protocol_query();
  query.source_node = "node-020";
  query.checkpoint_size_b = 239244449;
  query.vram_required_bytes = 14234047123ULL;
  query.cuda_capability_required = 8.0;
  query.remaining_runtime_s = 28310.494514477887;
  query.require_deadline = false;
  ASSERT_TRUE(hazard.observe_sample(
      "node-001", 0.02586751065149507 / query.remaining_runtime_s,
      query.decision_ns - 1));
  const auto selected =
      P2Selector(1, 2, 0.5, topology, hazard, nodes).select(query);
  ASSERT_TRUE(selected.success);
  const auto &candidate = selected.ranked.front();
  EXPECT_NEAR(candidate.T_transfer_s, 4.329420882434579, 1e-12);
  EXPECT_NEAR(candidate.survival_prob, 0.9744641871818349, 1e-12);
  EXPECT_NEAR(candidate.composite_cost, 5.298980624489991, 1e-12);
}

TEST(P2Protocol, UsesLocalPoolOnlyWhenTwoCompatibleLocalNodesExist) {
  TopologyGraph topology;
  HazardEstimator hazard;
  NodeRegistry nodes;
  nodes.upsert(protocol_node("source", "A"));
  for (const auto &id : {"local-1", "local-2", "remote"}) {
    topology.add_link({"source", id, id == std::string("remote") ? 1e9 : 1e7, 1,
                       TopologyLevel::INTRA_RACK});
    nodes.upsert(protocol_node(id, id == std::string("remote") ? "B" : "A"));
  }
  P2Selector selector(1, 2, 0.5, topology, hazard, nodes);
  auto query = protocol_query();
  auto result = selector.select(query);
  ASSERT_TRUE(result.success);
  EXPECT_NE(result.selected, "remote");
  auto local = nodes.get("local-2").value();
  local.telemetry.gpu_util_pct = 99;
  nodes.upsert(local);
  result = selector.select(query);
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.selected, "remote");
}

TEST(P2Protocol, CapabilityGpuCountAndLoadAreHardConstraints) {
  TopologyGraph topology;
  HazardEstimator hazard;
  NodeRegistry nodes;
  nodes.upsert(protocol_node("source", "A"));
  auto incapable = protocol_node("incapable", "B", 1024, 2, 7.5);
  auto no_gpu = protocol_node("no-gpu", "B", 1024, 1);
  auto loaded = protocol_node("loaded", "B", 1024, 2);
  loaded.telemetry.gpu_util_pct = 90;
  auto valid = protocol_node("valid", "B", 1024, 2);
  for (const auto &node : {incapable, no_gpu, loaded, valid}) {
    nodes.upsert(node);
    topology.add_link(
        {"source", node.node_id, 1e9, 1, TopologyLevel::INTRA_RACK});
  }
  auto query = protocol_query();
  query.cuda_capability_required = 8;
  query.gpu_count_required = 2;
  const auto result =
      P2Selector(1, 2, 0.5, topology, hazard, nodes).select(query);
  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.selected, "valid");
}

TEST(P2Protocol, HistoricalCostIgnoresFutureCompletions) {
  TopologyGraph topology;
  HazardEstimator hazard;
  NodeRegistry nodes;
  topology.add_link({"source", "dest", 1e9, 1, TopologyLevel::INTRA_RACK});
  nodes.upsert(protocol_node("source", "A"));
  nodes.upsert(protocol_node("dest", "B"));
  auto query = protocol_query();
  P2Selector selector(1, 2, 0.5, topology, hazard, nodes);
  ASSERT_TRUE(
      selector.observe_migration_cost("dest", 10, query.decision_ns - 1));
  ASSERT_TRUE(
      selector.observe_migration_cost("dest", 100000, query.decision_ns + 1));
  auto result = selector.select(query);
  ASSERT_TRUE(result.success);
  EXPECT_DOUBLE_EQ(result.ranked.front().historical_migration_cost_s, 10);
}

TEST(P2Protocol, GangMatchingAvoidsGreedyResourceFailure) {
  TopologyGraph topology;
  HazardEstimator hazard;
  NodeRegistry nodes;
  for (const auto &source : {"s0", "s1"}) {
    nodes.upsert(protocol_node(source, "source-building"));
    topology.add_link({source, "d0", 10000, 1, TopologyLevel::INTRA_RACK});
    topology.add_link({source, "d1", 8000, 1, TopologyLevel::INTRA_RACK});
  }
  nodes.upsert(protocol_node("d0", "dest-building", 200));
  nodes.upsert(protocol_node("d1", "dest-building", 100));
  auto q0 = protocol_query();
  q0.vram_required_bytes = 100;
  auto q1 = q0;
  q1.vram_required_bytes = 200;
  P2GangQuery query;
  query.source_nodes = {"s0", "s1"};
  query.rank_queries = {q0, q1};
  const auto result =
      P2Selector(1, 2, 0.5, topology, hazard, nodes).select_gang(query);
  ASSERT_TRUE(result.success);
  ASSERT_EQ(result.selected.size(), 2u);
  EXPECT_EQ(result.selected[0], "d1");
  EXPECT_EQ(result.selected[1], "d0");
}
