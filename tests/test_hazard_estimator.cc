#include "reclaimnet/capacity_estimator.h"
#include "reclaimnet/hazard_estimator.h"
#include "reclaimnet/time.h"
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <thread>
#include <vector>

using namespace reclaimnet;
using namespace reclaimnet::control;

namespace {
constexpr int64_t kSecond = 1000000000LL;
constexpr int64_t kStart = 1772302390292933888LL;
}

TEST(HazardEstimator, ZeroForUnknownNode) {
  HazardEstimator estimator;
  EXPECT_EQ(estimator.lambda("unknown-node"), 0.0);
  EXPECT_EQ(estimator.upper_bound("unknown-node", kStart), 0.0);
  EXPECT_FALSE(estimator.estimate_at("unknown-node", kStart));
  EXPECT_FALSE(estimator.confidence_upper_bound("unknown-node", kStart));
  EXPECT_DOUBLE_EQ(estimator.survival_prob("unknown-node"), 1.0);
}

TEST(HazardEstimator, CountsEmergenciesOverRegisteredExposure) {
  HazardEstimator estimator(100.0);
  ASSERT_TRUE(estimator.observe_node("A", kStart));
  estimator.record_event({"A", kStart + 20 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 40 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 50 * kSecond, ReclaimType::SCHEDULED});
  ASSERT_TRUE(estimator.estimate_at("A", kStart + 80 * kSecond));
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 80 * kSecond),
                   2.0 / 80.0);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 120 * kSecond),
                   1.0 / 100.0);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 150 * kSecond), 0.0);
}

TEST(HazardEstimator, FirstEventNeverUsesUnixEpochAsExposure) {
  HazardEstimator estimator(100.0);
  estimator.record_event({"A", kStart, ReclaimType::EMERGENCY});
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart), 0.01);
  EXPECT_FALSE(estimator.confidence_upper_bound("A", kStart));
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 20 * kSecond), 0.05);
  EXPECT_TRUE(estimator.confidence_upper_bound("A", kStart + 20 * kSecond));
}

TEST(HazardEstimator,
     ConfidenceBoundMatchesPoissonScoreAndRemainsPositiveWithNoEvents) {
  HazardEstimator estimator(100.0);
  estimator.configure_window(100.0, 2.0);
  ASSERT_TRUE(estimator.observe_node("A", kStart));
  EXPECT_DOUBLE_EQ(
      *estimator.confidence_upper_bound("A", kStart + 100 * kSecond), 0.04);
  estimator.record_event({"A", kStart + 20 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 40 * kSecond, ReclaimType::EMERGENCY});
  const double expected = (2.0 + 2.0 + 2.0 * std::sqrt(3.0)) / 100.0;
  EXPECT_DOUBLE_EQ(
      *estimator.confidence_upper_bound("A", kStart + 100 * kSecond), expected);
  EXPECT_DOUBLE_EQ(estimator.upper_bound("A", kStart + 100 * kSecond),
                   expected);
  EXPECT_GT(expected, *estimator.estimate_at("A", kStart + 100 * kSecond));
}

TEST(HazardEstimator,
     FutureAndOutOfOrderEventsAreCausalAndRetriesAreIdempotent) {
  HazardEstimator estimator(100.0);
  ASSERT_TRUE(estimator.observe_node("A", kStart));
  estimator.record_event({"A", kStart + 90 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 20 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 20 * kSecond, ReclaimType::EMERGENCY});
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 50 * kSecond),
                   1.0 / 50.0);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 100 * kSecond),
                   2.0 / 100.0);
  EXPECT_FALSE(estimator.estimate_at("A", kStart - 1));
}

TEST(HazardEstimator, SuppliedPointsKeepUnavailableConfidenceAndCausalHistory) {
  HazardEstimator estimator;
  ASSERT_TRUE(estimator.observe_sample("A", 0.9, kStart + 60 * kSecond));
  ASSERT_TRUE(estimator.observe_sample("A", 0.002, kStart));
  estimator.record_event({"A", kStart + 1, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart - 1, ReclaimType::EMERGENCY});
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 30 * kSecond), 0.002);
  EXPECT_FALSE(estimator.confidence_upper_bound("A", kStart + 30 * kSecond));
  EXPECT_DOUBLE_EQ(estimator.upper_bound("A", kStart + 30 * kSecond), 0.002);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 60 * kSecond), 0.9);
}

TEST(HazardEstimator, ScheduledEventsDoNotCreateEmergencyEvidence) {
  HazardEstimator estimator;
  estimator.record_event({"A", kStart, ReclaimType::SCHEDULED});
  EXPECT_FALSE(estimator.estimate_at("A", kStart));
}

TEST(HazardEstimator,
     ConfigurationChangesWindowWithoutDestroyingHistoricalQueries) {
  HazardEstimator estimator(100.0);
  ASSERT_TRUE(estimator.observe_node("A", kStart));
  estimator.record_event({"A", kStart + 20 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 80 * kSecond, ReclaimType::EMERGENCY});
  estimator.record_event({"A", kStart + 90 * kSecond, ReclaimType::EMERGENCY});
  estimator.configure_window(50.0, 1.96);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 100 * kSecond), 0.04);
  estimator.configure_window(100.0, 1.96);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 100 * kSecond), 0.03);
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 50 * kSecond), 0.02);
}

TEST(HazardEstimator, InvalidInputRejectedWithoutCreatingState) {
  HazardEstimator estimator;
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  const auto infinity = std::numeric_limits<double>::infinity();
  EXPECT_FALSE(estimator.observe_node("", kStart));
  EXPECT_FALSE(estimator.observe_node("A", 0));
  EXPECT_FALSE(estimator.observe_sample("A", nan, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", infinity, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", -1.0, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", 0.1, -1));
  estimator.record_event({"A", 0, ReclaimType::EMERGENCY});
  EXPECT_FALSE(estimator.estimate_at("A", kStart));
  EXPECT_THROW(HazardEstimator(0), std::invalid_argument);
  EXPECT_THROW(HazardEstimator{nan}, std::invalid_argument);
  EXPECT_THROW(estimator.configure_window(100, nan), std::invalid_argument);
  EXPECT_THROW(estimator.configure_window(100, -1), std::invalid_argument);
  EXPECT_THROW(estimator.survival_prob("A", -1), std::invalid_argument);
}

TEST(HazardEstimator, PruneAndSnapshotRetainMeaningfulNodeState) {
  HazardEstimator estimator;
  estimator.record_event({"old", kSecond, ReclaimType::EMERGENCY});
  ASSERT_TRUE(estimator.observe_sample("point", 0.002, now_ns()));
  ASSERT_TRUE(estimator.observe_node("active", now_ns() - kSecond));
  estimator.record_event({"active", now_ns(), ReclaimType::EMERGENCY});
  estimator.prune(100.0);
  EXPECT_FALSE(estimator.estimate("old"));
  const auto snapshot = estimator.snapshot();
  ASSERT_EQ(snapshot.size(), 2u);
  EXPECT_FALSE(snapshot.at("point").confidence_available);
  EXPECT_TRUE(snapshot.at("active").confidence_available);
  EXPECT_EQ(snapshot.at("active").event_count, 1u);
}

TEST(HazardEstimator, ConcurrentQueriesAndWritesPreserveAllEvents) {
  HazardEstimator estimator(100.0);
  ASSERT_TRUE(estimator.observe_node("A", kStart));
  std::vector<std::thread> threads;
  for (int shard = 0; shard < 4; ++shard) {
    threads.emplace_back([&, shard] {
      for (int i = 0; i < 100; ++i) {
        estimator.record_event(
            {"A", kStart + kSecond + shard * 100 + i, ReclaimType::EMERGENCY});
        estimator.estimate_at("A", kStart + 2 * kSecond);
        estimator.confidence_upper_bound("A", kStart + 2 * kSecond);
      }
    });
  }
  for (auto &thread : threads)
    thread.join();
  EXPECT_DOUBLE_EQ(*estimator.estimate_at("A", kStart + 2 * kSecond), 200.0);
}

TEST(CapacityEstimator, RollingMinimumUsesCausalWindowAndRetainsFutureSamples) {
  CapacityEstimator estimator(100.0, 0.8);
  ASSERT_TRUE(estimator.observe_sample("A", "B", 10.0, kStart + 150 * kSecond));
  ASSERT_TRUE(estimator.observe_sample("A", "B", 80.0, kStart));
  ASSERT_TRUE(estimator.observe_sample("A", "B", 100.0, kStart + 50 * kSecond));
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 80 * kSecond, 200.0), 64.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 100 * kSecond, 200.0), 80.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 150 * kSecond, 200.0), 8.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 80 * kSecond, 200.0), 64.0);
}

TEST(CapacityEstimator,
     ZeroAvailabilityAndSameTimestampWorstObservationArePreserved) {
  CapacityEstimator estimator(100.0, 0.8);
  ASSERT_TRUE(estimator.observe_sample("A", "B", 100.0, kStart));
  ASSERT_TRUE(estimator.observe_sample("A", "B", 0.0, kStart));
  ASSERT_TRUE(estimator.observe_sample("A", "B", 200.0, kStart));
  EXPECT_DOUBLE_EQ(estimator.lower_bound_bps("A", "B", kStart, 1000.0), 0.0);
}

TEST(CapacityEstimator,
     NominalFallbackIsSeparateFromAvailableMeasuredCapacity) {
  CapacityEstimator estimator(100.0, 0.8);
  EXPECT_DOUBLE_EQ(estimator.lower_bound_bps("A", "B", kStart, 1000.0), 1000.0);
  ASSERT_TRUE(estimator.observe_sample("A", "B", 200.0, kStart + 10 * kSecond));
  EXPECT_DOUBLE_EQ(estimator.lower_bound_bps("A", "B", kStart, 1000.0), 1000.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 10 * kSecond, 100.0), 100.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("B", "A", kStart + 10 * kSecond, 1000.0),
      1000.0);
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 110 * kSecond, 1000.0),
      1000.0);
}

TEST(CapacityEstimator, InvalidInputIsRejectedAndQueriesFailClosed) {
  CapacityEstimator estimator;
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(estimator.observe_sample("", "B", 100.0, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", "A", 100.0, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", "B", -1.0, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", "B", nan, kStart));
  EXPECT_FALSE(estimator.observe_sample("A", "B", 100.0, 0));
  EXPECT_DOUBLE_EQ(estimator.lower_bound_bps("A", "B", 0, 1000.0), 0.0);
  EXPECT_DOUBLE_EQ(estimator.lower_bound_bps("A", "B", kStart, nan), 0.0);
  EXPECT_THROW(CapacityEstimator(0), std::invalid_argument);
  EXPECT_THROW(CapacityEstimator(100, 0), std::invalid_argument);
  EXPECT_THROW(CapacityEstimator(100, 1.01), std::invalid_argument);
}

TEST(CapacityEstimator, ConcurrentQueriesAndObservationsPreserveMinimum) {
  CapacityEstimator estimator(100.0, 0.8);
  std::vector<std::thread> threads;
  for (int shard = 0; shard < 4; ++shard) {
    threads.emplace_back([&, shard] {
      for (int i = 0; i < 100; ++i) {
        estimator.observe_sample("A", "B", 10.0 + shard + i,
                                 kStart + kSecond + shard * 100 + i);
        estimator.lower_bound_bps("A", "B", kStart + 2 * kSecond, 1000.0);
      }
    });
  }
  for (auto &thread : threads)
    thread.join();
  EXPECT_DOUBLE_EQ(
      estimator.lower_bound_bps("A", "B", kStart + 2 * kSecond, 1000.0), 8.0);
}
