#include "reclaimnet/p1_allocator.h"
#include <gtest/gtest.h>

#include <cmath>
#include <limits>

using namespace reclaimnet;
using namespace reclaimnet::control;

TEST(P1Allocator, OptimalIntervalUsesBytesAndBitsPerSecond) {
  const double dt = P1Allocator::optimal_interval_s(1.0 / 3600.0, 1e9, 1e10);
  EXPECT_NEAR(dt, std::sqrt(16.0 * 1e9 / (1.0 / 3600.0 * 1e10)), 1e-9);
}

TEST(P1Allocator, TraceV3FirstAllocationEpoch) {

  P1Allocator allocator(370315888.0);
  const auto result = allocator.compute(
      {{"job-065", 1.0027484094100176e-06, 239244449, 370315888.0}});
  ASSERT_EQ(result.schedules.size(), 1u);
  const auto &schedule = result.schedules.front();
  ASSERT_TRUE(schedule.feasible);
  EXPECT_NEAR(schedule.b_i_bps, 370315888.0, 1e-6);
  EXPECT_NEAR(schedule.write_rate_limit_bps, 74063177.60000001, 1e-6);
  EXPECT_NEAR(schedule.raw_delta_t_s, 3210.692863638208, 1e-9);
  EXPECT_NEAR(schedule.lower_bound_s, 25.842201941926938, 1e-9);
  EXPECT_DOUBLE_EQ(schedule.upper_bound_s, 36000.0);
  EXPECT_NEAR(schedule.delta_t_s, 3210.692863638208, 1e-9);
}

TEST(P1Allocator, TraceV3DistributedJobUsesLogicalSizeAndAggregateHazard) {
  const double components[] = {2.3239432422986133e-6, 2.721181906450274e-6,
                               2.339217798080195e-6, 2.711034100107159e-6};
  double lambda = 0;
  for (double component : components)
    lambda += component;
  const auto result =
      P1Allocator(471185102.66201085)
          .compute({{"job-083", lambda, 7076779740ULL, 1148766268.0}});
  ASSERT_EQ(result.schedules.size(), 1u);
  const auto &schedule = result.schedules.front();
  ASSERT_TRUE(schedule.feasible);
  EXPECT_NEAR(lambda, 1.009537704693624e-5, 1e-20);
  EXPECT_NEAR(schedule.b_i_bps, 471185102.66201085, 1e-6);
  EXPECT_NEAR(schedule.write_rate_limit_bps, 94237020.53240217, 1e-6);
  EXPECT_NEAR(schedule.raw_delta_t_s, 4878.887266578238, 1e-8);
  EXPECT_NEAR(schedule.lower_bound_s, 600.7643026079536, 1e-9);
  EXPECT_NEAR(schedule.delta_t_s, 4878.887266578238, 1e-8);
}

TEST(P1Allocator, PermanentlyFixesCascadingCaps) {
  P1Allocator allocator(300.0);
  const auto result = allocator.compute(
      {{"a", 1.0, 1, 50.0}, {"b", 1.0, 1, 100.0}, {"c", 1.0, 1, 0.0}});
  ASSERT_EQ(result.schedules.size(), 3u);
  EXPECT_DOUBLE_EQ(result.schedules[0].b_i_bps, 50.0);
  EXPECT_DOUBLE_EQ(result.schedules[1].b_i_bps, 100.0);
  EXPECT_DOUBLE_EQ(result.schedules[2].b_i_bps, 150.0);
  EXPECT_DOUBLE_EQ(result.total_bps_used, 300.0);
  EXPECT_LE(result.water_fill_iters, 4);
}

TEST(P1Allocator, CubeRootWeightsDetermineAllocationSlices) {
  P1Allocator allocator(60.0);
  const auto result = allocator.compute(
      {{"a", 1.0, 1, 0.0}, {"b", 8.0, 1, 0.0}, {"c", 27.0, 1, 0.0}});
  ASSERT_EQ(result.schedules.size(), 3u);
  EXPECT_NEAR(result.schedules[0].b_i_bps, 10.0, 1e-12);
  EXPECT_NEAR(result.schedules[1].b_i_bps, 20.0, 1e-12);
  EXPECT_NEAR(result.schedules[2].b_i_bps, 30.0, 1e-12);
}

TEST(P1Allocator, LeavesUnusedBudgetWhenEveryJobIsCapped) {
  const auto result =
      P1Allocator(100.0).compute({{"a", 1.0, 1, 10.0}, {"b", 1.0, 1, 10.0}});
  EXPECT_DOUBLE_EQ(result.total_bps_used, 20.0);
  EXPECT_DOUBLE_EQ(result.schedules[0].b_i_bps, 10.0);
  EXPECT_DOUBLE_EQ(result.schedules[1].b_i_bps, 10.0);
}

TEST(P1Allocator, ZeroHazardIsBoundedByLossBudget) {
  const auto result =
      P1Allocator(1e9).compute({{"zero-risk", 0.0, 1000000, 0.0}});
  const auto &schedule = result.schedules.front();
  ASSERT_TRUE(schedule.feasible);
  EXPECT_TRUE(std::isinf(schedule.raw_delta_t_s));
  EXPECT_DOUBLE_EQ(schedule.delta_t_s, 36000.0);
}

TEST(P1Allocator, EqualSharesAndCapsWhenAllHazardsAreZero) {
  const auto result = P1Allocator(9.0).compute(
      {{"a", 0.0, 1, 1.0}, {"b", 0.0, 1, 0.0}, {"c", 0.0, 1, 0.0}});
  EXPECT_DOUBLE_EQ(result.schedules[0].b_i_bps, 1.0);
  EXPECT_DOUBLE_EQ(result.schedules[1].b_i_bps, 4.0);
  EXPECT_DOUBLE_EQ(result.schedules[2].b_i_bps, 4.0);
  for (const auto &schedule : result.schedules)
    EXPECT_TRUE(schedule.feasible);
}

TEST(P1Allocator, EnforcesCheckpointDurationLowerBound) {
  const auto result =
      P1Allocator(1000.0).compute({{"high-risk", 1.0, 1000, 0.0}});
  const auto &schedule = result.schedules.front();
  ASSERT_TRUE(schedule.feasible);
  EXPECT_DOUBLE_EQ(schedule.write_rate_limit_bps, 200.0);
  EXPECT_DOUBLE_EQ(schedule.lower_bound_s, 40.0);
  EXPECT_DOUBLE_EQ(schedule.delta_t_s, 40.0);
  EXPECT_LT(schedule.raw_delta_t_s, schedule.lower_bound_s);
}

TEST(P1Allocator, AppliesConfigurableLossBudgetUpperBound) {
  P1JobInput job{"rare-reclaim", 1e-12, 1000, 0.0};
  job.loss_budget_s = 100.0;
  const auto result = P1Allocator(1000.0).compute({job});
  ASSERT_TRUE(result.schedules.front().feasible);
  EXPECT_DOUBLE_EQ(result.schedules.front().delta_t_s, 200.0);
}

TEST(P1Allocator, ReportsInfeasibleIntervalBounds) {
  const auto result =
      P1Allocator(1.0).compute({{"cannot-checkpoint", 0.001, 1000, 0.0}});
  const auto &schedule = result.schedules.front();
  EXPECT_FALSE(schedule.feasible);
  EXPECT_GT(schedule.lower_bound_s, schedule.upper_bound_s);
  EXPECT_DOUBLE_EQ(schedule.delta_t_s, 0.0);
  EXPECT_FALSE(schedule.reason.empty());
}

TEST(P1Allocator, ZeroBudgetDoesNotInventAnInterval) {
  const auto result =
      P1Allocator(0.0).compute({{"no-budget", 0.001, 1000, 0.0}});
  EXPECT_DOUBLE_EQ(result.total_bps_used, 0.0);
  EXPECT_FALSE(result.schedules.front().feasible);
  EXPECT_DOUBLE_EQ(result.schedules.front().delta_t_s, 0.0);
  EXPECT_TRUE(std::isinf(result.schedules.front().lower_bound_s));
}

TEST(P1Allocator, InvalidHazardCannotContaminateOtherJobs) {
  const auto result = P1Allocator(100.0).compute(
      {{"nan", std::numeric_limits<double>::quiet_NaN(), 1, 0.0},
       {"negative", -1.0, 1, 0.0},
       {"valid", 1.0, 1, 0.0}});
  EXPECT_FALSE(result.schedules[0].feasible);
  EXPECT_FALSE(result.schedules[1].feasible);
  EXPECT_DOUBLE_EQ(result.schedules[0].b_i_bps, 0.0);
  EXPECT_DOUBLE_EQ(result.schedules[1].b_i_bps, 0.0);
  EXPECT_DOUBLE_EQ(result.schedules[2].b_i_bps, 100.0);
  EXPECT_TRUE(result.schedules[2].feasible);
}

TEST(P1Allocator, InvalidBetaOrBudgetIsInfeasible) {
  P1JobInput job{"invalid-beta", 1.0, 1, 0.0};
  job.checkpoint_beta = 0.0;
  EXPECT_FALSE(P1Allocator(100.0).compute({job}).schedules.front().feasible);
  job.checkpoint_beta = 0.2;
  EXPECT_FALSE(P1Allocator(-1.0).compute({job}).schedules.front().feasible);
  EXPECT_FALSE(P1Allocator(std::numeric_limits<double>::infinity())
                   .compute({job})
                   .schedules.front()
                   .feasible);
}

TEST(P1Allocator, EmptyInput) {
  const auto result = P1Allocator(100.0).compute({});
  EXPECT_TRUE(result.schedules.empty());
  EXPECT_DOUBLE_EQ(result.total_bps_used, 0.0);
}
