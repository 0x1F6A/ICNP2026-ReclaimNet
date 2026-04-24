


#include <gtest/gtest.h>
#include "reclaimnet/p1_allocator.h"

using namespace reclaimnet;
using namespace reclaimnet::control;



TEST(P1Allocator, OptimalIntervalBasic) {



  double dt = P1Allocator::optimal_interval_s(1.0/3600.0, 1e9, 1e10);
  EXPECT_GT(dt, 0.0);
  EXPECT_NEAR(dt, std::sqrt(2.0 * 1e9 / (1.0/3600.0 * 1e10)), 0.01);
}

TEST(P1Allocator, OptimalIntervalZeroLambda) {
  double dt = P1Allocator::optimal_interval_s(0.0, 1e9, 1e9);
  EXPECT_EQ(dt, 300.0);
}

TEST(P1Allocator, OptimalIntervalZeroBandwidth) {
  double dt = P1Allocator::optimal_interval_s(0.001, 1e9, 0.0);
  EXPECT_EQ(dt, 300.0);
}



TEST(P1Allocator, SingleJobGetsBudget) {
  P1Allocator alloc(1e9);
  P1JobInput j;
  j.job_id            = "job-1";
  j.lambda_i          = 1e-4;
  j.checkpoint_size_b = 2000000000ULL;
  j.write_cap_bps     = 0.0;

  auto r = alloc.compute({j});
  ASSERT_EQ(r.schedules.size(), 1u);
  EXPECT_NEAR(r.schedules[0].b_i_bps, 1e9, 1.0);
  EXPECT_GT(r.schedules[0].delta_t_s, 0.0);
}



TEST(P1Allocator, TwoJobsProportional) {
  P1Allocator alloc(2e9);
  P1JobInput j1, j2;
  j1.job_id = "job-1"; j1.lambda_i = 1e-3; j1.checkpoint_size_b = 1000000000; j1.write_cap_bps = 0;
  j2.job_id = "job-2"; j2.lambda_i = 1e-3; j2.checkpoint_size_b = 1000000000; j2.write_cap_bps = 0;

  auto r = alloc.compute({j1, j2});
  ASSERT_EQ(r.schedules.size(), 2u);

  EXPECT_NEAR(r.schedules[0].b_i_bps, r.schedules[1].b_i_bps, 1.0);
  EXPECT_NEAR(r.total_bps_used, 2e9, 1.0);
}



TEST(P1Allocator, WaterFillingCap) {
  P1Allocator alloc(2e9);
  P1JobInput j1, j2;
  j1.job_id = "job-1"; j1.lambda_i = 1e-3; j1.checkpoint_size_b = 1000000000;
  j1.write_cap_bps = 5e8;
  j2.job_id = "job-2"; j2.lambda_i = 1e-3; j2.checkpoint_size_b = 1000000000;
  j2.write_cap_bps = 0.0;

  auto r = alloc.compute({j1, j2});
  ASSERT_EQ(r.schedules.size(), 2u);

  double b1 = 0.0, b2 = 0.0;
  for (auto& s : r.schedules) {
    if (s.job_id == "job-1") b1 = s.b_i_bps;
    else                      b2 = s.b_i_bps;
  }
  EXPECT_LE(b1, 5e8 + 1.0);

  EXPECT_GT(b2, 1e9);
}



TEST(P1Allocator, EmptyInput) {
  P1Allocator alloc(1e9);
  auto r = alloc.compute({});
  EXPECT_TRUE(r.schedules.empty());
  EXPECT_EQ(r.total_bps_used, 0.0);
}
