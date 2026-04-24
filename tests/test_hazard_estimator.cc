


#include <gtest/gtest.h>
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/time.h"

#include <thread>
#include <chrono>

using namespace reclaimnet;
using namespace reclaimnet::control;

TEST(HazardEstimator, ZeroForUnknownNode) {
  HazardEstimator he(3600.0);
  EXPECT_EQ(he.lambda("unknown-node"), 0.0);
  EXPECT_NEAR(he.survival_prob("unknown-node"), 1.0, 1e-9);
}

TEST(HazardEstimator, FirstEventInitialises) {
  HazardEstimator he(3600.0);
  he.record_event({"node-A", now_ns(), ReclaimType::VOLUNTARY});
  EXPECT_GT(he.lambda("node-A"), 0.0);
}

TEST(HazardEstimator, SurvivalProbDecaysWithLambda) {
  HazardEstimator he(3600.0);

  int64_t t = now_ns();
  for (int i = 0; i < 10; ++i) {
    he.record_event({"node-B", t + (int64_t)i * 1000000000LL, ReclaimType::FORCED});
  }
  double sp = he.survival_prob("node-B", 3600.0);
  EXPECT_GE(sp, 0.0);
  EXPECT_LE(sp, 1.0);

  EXPECT_LT(sp, 0.01);
}

TEST(HazardEstimator, PruneRemovesOldEntries) {
  HazardEstimator he(3600.0);

  he.record_event({"node-C", 1000000000LL, ReclaimType::VOLUNTARY});
  EXPECT_GT(he.lambda("node-C"), 0.0);

  he.prune(1e-9);
  EXPECT_EQ(he.lambda("node-C"), 0.0);
}

TEST(HazardEstimator, Snapshot) {
  HazardEstimator he(3600.0);
  he.record_event({"n1", now_ns(), ReclaimType::VOLUNTARY});
  he.record_event({"n2", now_ns(), ReclaimType::VOLUNTARY});
  auto snap = he.snapshot();
  EXPECT_EQ(snap.size(), 2u);
}
