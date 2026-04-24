


#include <gtest/gtest.h>
#include "reclaimnet/p2_selector.h"
#include "reclaimnet/topology.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/node_registry.h"
#include "reclaimnet/time.h"

using namespace reclaimnet;
using namespace reclaimnet::control;



class P2Test : public ::testing::Test {
protected:
  void SetUp() override {

    Link lk1{"node-src", "node-dst1", 100e9, 2.0, TopologyLevel::IntraRack};
    Link lk2{"node-src", "node-dst2", 40e9,  10.0, TopologyLevel::InterRack};
    topo.add_link(lk1);
    topo.add_link(lk2);


    NodeRecord r1{};
    r1.node_id         = "node-dst1";
    r1.state           = NodeState::ACTIVE;
    r1.link_capacity_bps = (BandwidthBps)100e9;
    r1.telemetry.vram_free_bytes = 16ULL * 1024 * 1024 * 1024;
    r1.telemetry.updated_ns      = now_ns();
    nodes.upsert(r1);

    NodeRecord r2{};
    r2.node_id         = "node-dst2";
    r2.state           = NodeState::ACTIVE;
    r2.link_capacity_bps = (BandwidthBps)40e9;
    r2.telemetry.vram_free_bytes = 16ULL * 1024 * 1024 * 1024;
    r2.telemetry.updated_ns      = now_ns();
    nodes.upsert(r2);
  }

  TopologyGraph    topo;
  HazardEstimator  hazard;
  NodeRegistry     nodes;
};



TEST_F(P2Test, SelectsBestDestination) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id             = "job-1";
  q.source_node        = "node-src";
  q.checkpoint_size_b  = 1000000000ULL;
  q.reclaim_deadline_s = 300.0;
  q.T_restore_s        = 5.0;

  auto r = sel.select(q);
  EXPECT_TRUE(r.success);
  EXPECT_FALSE(r.selected.empty());
  EXPECT_GE(r.ranked.size(), 2u);
}



TEST_F(P2Test, InfeasibleTightDeadline) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id             = "job-tight";
  q.source_node        = "node-src";
  q.checkpoint_size_b  = 400000000000ULL;
  q.reclaim_deadline_s = 0.1;
  q.T_restore_s        = 0.0;

  auto r = sel.select(q);
  EXPECT_FALSE(r.success);
}



TEST(P2Selector, TopologyDistScore) {
  EXPECT_EQ(P2Selector::topology_dist_score(TopologyLevel::NVLink),    0.0);
  EXPECT_EQ(P2Selector::topology_dist_score(TopologyLevel::WAN),       1.0);
  EXPECT_GT(P2Selector::topology_dist_score(TopologyLevel::InterRack),
            P2Selector::topology_dist_score(TopologyLevel::IntraRack));
}



TEST_F(P2Test, RankingBestFirst) {
  P2Selector sel(1.0, 2.0, 0.5, topo, hazard, nodes);

  P2Query q;
  q.job_id             = "job-rank";
  q.source_node        = "node-src";
  q.checkpoint_size_b  = 500000000ULL;
  q.reclaim_deadline_s = 200.0;
  q.T_restore_s        = 1.0;

  auto r = sel.select(q);
  ASSERT_GE(r.ranked.size(), 2u);

  for (size_t i = 0; i + 1 < r.ranked.size(); ++i) {
    if (r.ranked[i].feasible && r.ranked[i+1].feasible) {
      EXPECT_LE(r.ranked[i].composite_cost, r.ranked[i+1].composite_cost);
    }
  }
}
