#include "reclaimnet/p3_admission.h"
#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <limits>
#include <thread>

using namespace reclaimnet;
using namespace reclaimnet::control;

namespace {

constexpr int64_t kDecisionNs = 1772302390292933888LL;

MigrationEntry make_entry(const std::string &job, const std::string &src = "A",
                          const std::string &dst = "B", uint64_t bytes = 1000,
                          double notice_s = 10.0, double restore_s = 2.0) {
  MigrationEntry entry;
  entry.job_id = job;
  entry.src = src;
  entry.dst = dst;
  entry.data_remaining_bytes = bytes;
  entry.deadline_s = notice_s;
  entry.start_ns = kDecisionNs;
  entry.restore_time_s = restore_s;
  return entry;
}

}

TEST(P3Admission, ReturnsAllocatedBitRateAndAbsoluteNanosecondDeadline) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  const auto result = admission.admit(make_entry("job"));
  ASSERT_TRUE(result);
  EXPECT_DOUBLE_EQ(result.value().allocated_bps, 1000.0);
  EXPECT_EQ(result.value().deadline_ns, kDecisionNs + 10000000000LL);
  EXPECT_EQ(result.value().data_total_bytes, 1000u);
  admission.release("job");
  EXPECT_EQ(admission.active_count(), 0u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 0.0);
}

TEST(P3Admission, ConcurrentJobsReserveOnlyTheirDeadlineRate) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  ASSERT_TRUE(admission.admit(make_entry("first")));
  ASSERT_TRUE(admission.admit(make_entry("second")));
  EXPECT_EQ(admission.active_count(), 2u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 2000.0);
  admission.release("first");
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission,
     RejectsInsufficientDeadlineCapacityWithoutChangingReservations) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 2000.0);
  ASSERT_TRUE(admission.admit(make_entry("first")));
  EXPECT_FALSE(admission.admit(make_entry("second")));
  EXPECT_EQ(admission.active_count(), 1u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission, DefaultsToTraceResearchFractionAndMinimumReserve) {
  P3Admission admission;
  admission.add_link("A", "B", 100000000.0);

  const auto result = admission.admit(
      make_entry("fills-budget", "A", "B", 62500000, 10.0, 0.0));
  ASSERT_TRUE(result);
  EXPECT_DOUBLE_EQ(result.value().allocated_bps, 50000000.0);
  EXPECT_FALSE(admission.admit(make_entry("extra", "A", "B", 1, 10.0, 0.0)));
}

TEST(P3Admission, TraceResearchFractionProtectsLargerLinks) {
  P3Admission admission;
  admission.add_link("A", "B", 1000000000.0);
  ASSERT_TRUE(admission.admit(
      make_entry("fills-budget", "A", "B", 875000000, 10.0, 0.0)));
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 700000000.0);
  EXPECT_FALSE(admission.admit(make_entry("extra", "A", "B", 1, 10.0, 0.0)));
}

TEST(P3Admission, DuplicateAdmissionIsIdempotentAfterProgress) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  const auto request = make_entry("same");
  ASSERT_TRUE(admission.admit(request));
  admission.update_progress("same", 500);
  const auto retry = admission.admit(request);
  ASSERT_TRUE(retry);
  EXPECT_EQ(retry.value().bytes_done, 500u);
  EXPECT_EQ(retry.value().data_remaining_bytes, 500u);
  EXPECT_EQ(admission.active_count(), 1u);
  EXPECT_EQ(admission.link_states().front().active_count, 1u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission, RetriesWithoutAnExplicitStartReuseOriginalDeadline) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  auto request = make_entry("same");
  request.start_ns = 0;
  const auto first = admission.admit(request);
  ASSERT_TRUE(first);
  const auto second = admission.admit(request);
  ASSERT_TRUE(second);
  EXPECT_EQ(first.value().deadline_ns, second.value().deadline_ns);
  EXPECT_EQ(admission.link_states().front().active_count, 1u);
}

TEST(P3Admission, ConflictingDuplicateCannotLeakAnotherLinksBudget) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  admission.add_link("C", "D", 10000.0);
  ASSERT_TRUE(admission.admit(make_entry("same")));
  EXPECT_FALSE(admission.admit(make_entry("same", "C", "D")));
  admission.release("same");
  for (const auto &link : admission.link_states()) {
    EXPECT_DOUBLE_EQ(link.allocated_bps, 0.0);
    EXPECT_EQ(link.active_count, 0u);
  }
}

TEST(P3Admission, CumulativeProgressIsIdempotentAndMonotonic) {
  P3Admission admission(0.7, 0.0);
  admission.add_link("A", "B", 10000.0);
  ASSERT_TRUE(admission.admit(make_entry("job")));
  admission.update_progress("job", 400);
  admission.update_progress("job", 400);
  admission.update_progress("job", 200);
  auto active = admission.active_migrations();
  ASSERT_EQ(active.size(), 1u);
  EXPECT_EQ(active.front().bytes_done, 400u);
  EXPECT_EQ(active.front().data_remaining_bytes, 600u);
  admission.update_progress("job", std::numeric_limits<uint64_t>::max());
  active = admission.active_migrations();
  EXPECT_EQ(active.front().bytes_done, 1000u);
  EXPECT_EQ(active.front().data_remaining_bytes, 0u);
}

TEST(P3Admission, SubsecondDeadlinePreservesUnixNanosecondPrecision) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 1000000.0);
  auto request = make_entry("job", "A", "B", 1000, 0.250000001, 0.0);
  request.minimum_useful_transfer_s = 0.01;
  const auto result = admission.admit(request);
  ASSERT_TRUE(result);
  const int64_t deadline = kDecisionNs + 250000001LL;
  EXPECT_EQ(result.value().deadline_ns, deadline);
  EXPECT_TRUE(admission.expired_migrations(kDecisionNs).empty());
  EXPECT_TRUE(admission.expired_migrations(deadline - 1).empty());
  ASSERT_EQ(admission.expired_migrations(deadline).size(), 1u);
}

TEST(P3Admission, RejectsInvalidNoticeRestoreAndDeadlineOverflow) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 1000000.0);
  EXPECT_FALSE(admission.admit(make_entry("zero", "A", "B", 1, 0.0, 0.0)));
  EXPECT_FALSE(admission.admit(make_entry("restore", "A", "B", 1, 1.0, 1.0)));
  EXPECT_FALSE(admission.admit(make_entry(
      "nan", "A", "B", 1, std::numeric_limits<double>::quiet_NaN(), 0.0)));
  auto overflow = make_entry("overflow");
  overflow.start_ns = std::numeric_limits<int64_t>::max() - 1;
  EXPECT_FALSE(admission.admit(overflow));
  EXPECT_EQ(admission.active_count(), 0u);
}

TEST(P3Admission, UnknownLinkIsRejected) {
  P3Admission admission(1.0, 0.0);
  EXPECT_FALSE(admission.admit(make_entry("unknown")));
}

TEST(P3Admission, ReservesAndReleasesEveryEdgeOfAMultiHopPath) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "switch", 3000.0);
  admission.add_link("switch", "B", 1000.0);
  auto request = make_entry("job");
  request.path_nodes = {"A", "switch", "B"};
  ASSERT_TRUE(admission.admit(request));
  for (const auto &link : admission.link_states()) {
    EXPECT_DOUBLE_EQ(link.allocated_bps, 1000.0);
    EXPECT_EQ(link.active_count, 1u);
  }
  request.job_id = "blocked";
  EXPECT_FALSE(admission.admit(request));
  admission.release("job");
  for (const auto &link : admission.link_states()) {
    EXPECT_DOUBLE_EQ(link.allocated_bps, 0.0);
    EXPECT_EQ(link.active_count, 0u);
  }
}

TEST(P3Admission, FailedMultiHopAdmissionDoesNotPartiallyReserve) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "switch", 3000.0);
  admission.add_link("switch", "B", 999.0);
  auto request = make_entry("job");
  request.path_nodes = {"A", "switch", "B"};
  EXPECT_FALSE(admission.admit(request));
  for (const auto &link : admission.link_states()) {
    EXPECT_DOUBLE_EQ(link.allocated_bps, 0.0);
    EXPECT_EQ(link.active_count, 0u);
  }
}

TEST(P3Admission, ReverseDirectionSharesTheSamePhysicalBudget) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 1500.0);
  ASSERT_TRUE(admission.admit(make_entry("first")));
  admission.add_link("B", "A", 1500.0);
  EXPECT_FALSE(admission.admit(make_entry("reverse", "B", "A")));
  EXPECT_EQ(admission.link_states().size(), 1u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission, DistinctNodeIdsContainingSeparatorsDoNotCollide) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A:B", "C", 1000.0);
  admission.add_link("A", "B:C", 1000.0);
  ASSERT_TRUE(admission.admit(make_entry("one", "A:B", "C")));
  ASSERT_TRUE(admission.admit(make_entry("two", "A", "B:C")));
  EXPECT_EQ(admission.active_count(), 2u);
}

TEST(P3Admission, InvalidPathAndInconsistentAbsoluteDeadlineAreRejected) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 10000.0);
  auto request = make_entry("cycle");
  request.path_nodes = {"A", "B", "A", "B"};
  EXPECT_FALSE(admission.admit(request));
  request.path_nodes = {"wrong", "B"};
  EXPECT_FALSE(admission.admit(request));
  request.path_nodes.clear();
  request.deadline_ns = kDecisionNs + 1;
  EXPECT_FALSE(admission.admit(request));
}

TEST(P3Admission, ConcurrentAdmissionCannotOverbookAnEdge) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 1000.0);
  std::atomic<int> accepted{0};
  std::thread first([&] {
    if (admission.admit(make_entry("first")))
      ++accepted;
  });
  std::thread second([&] {
    if (admission.admit(make_entry("second")))
      ++accepted;
  });
  first.join();
  second.join();
  EXPECT_EQ(accepted.load(), 1);
  EXPECT_EQ(admission.active_count(), 1u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission, EmergencyDrainPreservesTheExistingReservation) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 1000.0);
  ASSERT_TRUE(admission.admit(make_entry("job")));
  admission.emergency_drain("A");
  EXPECT_TRUE(admission.active_migrations().front().emergency);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 1000.0);
}

TEST(P3Admission, LastReleaseClearsFractionalRateRoundingResidue) {
  P3Admission admission(1.0, 0.0);
  admission.add_link("A", "B", 10.0);
  for (const auto &job : {"one", "two", "three"})
    ASSERT_TRUE(admission.admit(make_entry(job, "A", "B", 1, 3.0, 0.0)));
  for (const auto &job : {"one", "two", "three"})
    admission.release(job);
  EXPECT_EQ(admission.link_states().front().active_count, 0u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 0.0);
}

TEST(P3Batch, TraceV3FirstMigrationMaxMinRate) {
  P3Admission admission;
  admission.add_link("node-020", "sw-B-r2", 370315888.0);
  admission.add_link("sw-B-r2", "sw-B-core", 1e10);
  admission.add_link("sw-B-core", "sw-B-r1", 1e10);
  admission.add_link("sw-B-r1", "node-011", 500523159.0);
  auto entry = make_entry("job-065", "node-020", "node-011", 109057577,
                          275.034333, 275.034333 - 273.7697963321113);
  entry.path_nodes = {"node-020", "sw-B-r2", "sw-B-core", "sw-B-r1",
                      "node-011"};
  entry.migration_id = "migration-0001";
  const auto result = admission.admit_batch({entry}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 1u);
  EXPECT_TRUE(result.degraded.empty());
  EXPECT_NEAR(result.admitted.front().minimum_required_bps, 3186840.285849555,
              1e-7);
  EXPECT_NEAR(result.admitted.front().allocated_bps, 259221121.60000002, 1e-6);
  EXPECT_EQ(result.admitted.front().deadline_ns, 1772302665327266888LL);
}

TEST(P3Batch, TraceV3DistributedGangHasUniformBarrierRate) {
  P3Admission admission;
  const std::vector<double> capacity = {
      159539844.8 / 0.7, 151378673.6 / 0.7,       167701606.10000002 / 0.7,
      147358438.5 / 0.7, 162866790.8 / 0.7,       154808968.3 / 0.7,
      168268783.9 / 0.7, 161720984.60000002 / 0.7};
  const std::vector<std::pair<std::string, std::string>> access = {
      {"node-048", "sw-D-r1"}, {"node-047", "sw-D-r1"}, {"node-044", "sw-D-r1"},
      {"node-049", "sw-D-r2"}, {"node-045", "sw-D-r1"}, {"node-051", "sw-D-r2"},
      {"node-050", "sw-D-r2"}, {"node-052", "sw-D-r2"}};
  for (size_t i = 0; i < access.size(); ++i)
    admission.add_link(access[i].first, access[i].second, capacity[i]);
  admission.add_link("sw-D-r1", "sw-D-core", 1e10);
  admission.add_link("sw-D-core", "sw-D-r2", 1e10);
  const int64_t now = 1772607915376477952LL;
  const std::vector<std::vector<std::string>> paths = {
      {"node-048", "sw-D-r1", "node-047"},
      {"node-044", "sw-D-r1", "sw-D-core", "sw-D-r2", "node-049"},
      {"node-045", "sw-D-r1", "sw-D-core", "sw-D-r2", "node-051"},
      {"node-050", "sw-D-r2", "node-052"}};
  std::vector<MigrationEntry> entries;
  for (uint32_t rank = 0; rank < 4; ++rank) {
    auto entry = make_entry("job-233", paths[rank].front(), paths[rank].back(),
                            rank < 2 ? 21003093 : 21003092, 41.333648,
                            41.333648 - 27.095117275606444);
    entry.start_ns = now;
    entry.rank = rank;
    entry.gang_size = 4;
    entry.migration_id = "migration-0047";
    entry.path_nodes = paths[rank];
    entries.push_back(entry);
  }
  const auto result = admission.admit_batch(entries, now);
  ASSERT_EQ(result.admitted.size(), 4u);
  EXPECT_TRUE(result.degraded.empty());
  for (const auto &entry : result.admitted)
    EXPECT_NEAR(entry.allocated_bps, 147358438.5, 1e-6);
}

TEST(P3Batch, EarliestDeadlineOverloadAdmissionPreemptsLaterRequest) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  auto late = make_entry("late", "A", "B", 600, 10, 0);
  auto early = make_entry("early", "A", "B", 600, 8, 0);
  const auto result = admission.admit_batch({late, early}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 1u);
  EXPECT_EQ(result.admitted.front().job_id, "early");
  ASSERT_EQ(result.degraded.size(), 1u);
  EXPECT_EQ(result.degraded.front().entry.job_id, "late");
  EXPECT_FALSE(result.degraded.front().reason.empty());
  EXPECT_EQ(admission.degraded_migrations().size(), 1u);
}

TEST(P3Batch, MaxMinEqualizesUnequalMinimumRates) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  auto small = make_entry("small", "A", "B", 100, 8, 0);
  auto large = make_entry("large", "A", "B", 400, 8, 0);
  const auto result = admission.admit_batch({small, large}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 2u);
  for (const auto &entry : result.admitted)
    EXPECT_NEAR(entry.allocated_bps, 500, 1e-9);
}

TEST(P3Batch, FlowCapsRedistributeSurplus) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  auto capped = make_entry("capped", "A", "B", 100, 8, 0);
  capped.flow_cap_bps = 200;
  auto other = make_entry("other", "A", "B", 100, 8, 0);
  auto result = admission.admit_batch({capped, other}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 2u);
  EXPECT_NEAR(admission.get_reservation("capped")->allocated_bps, 200, 1e-9);
  EXPECT_NEAR(admission.get_reservation("other")->allocated_bps, 800, 1e-9);
}

TEST(P3Batch, ProgressRebalanceReturnsVersionedRateUpdates) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  auto first = make_entry("first", "A", "B", 400, 8, 0);
  auto second = make_entry("second", "A", "B", 400, 8, 0);
  ASSERT_EQ(admission.admit_batch({first, second}, kDecisionNs).admitted.size(),
            2u);
  const auto old = admission.get_reservation("first")->allocation_version;
  admission.update_progress("first", 400);
  const auto result = admission.admit_batch({}, kDecisionNs + 1000000000LL);
  EXPECT_EQ(result.rate_updates.size(), 2u);
  EXPECT_DOUBLE_EQ(admission.get_reservation("first")->allocated_bps, 0);
  EXPECT_DOUBLE_EQ(admission.get_reservation("second")->allocated_bps, 1000);
  EXPECT_GT(admission.get_reservation("first")->allocation_version, old);
}

TEST(P3Batch, GangFailureRollsBackLinksAndDestinationResources) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 10000);
  admission.set_destination_resources("B", 100, 1);
  auto first = make_entry("gang", "A", "B", 100, 8, 0);
  first.migration_id = "gang-id";
  first.rank = 0;
  first.gang_size = 2;
  first.vram_required_bytes = 60;
  first.gpu_count_required = 1;
  auto second = first;
  second.rank = 1;
  const auto result = admission.admit_batch({first, second}, kDecisionNs);
  EXPECT_TRUE(result.admitted.empty());
  EXPECT_EQ(result.degraded.size(), 2u);
  EXPECT_DOUBLE_EQ(admission.link_states().front().allocated_bps, 0);
  auto valid = make_entry("valid", "A", "B", 100, 8, 0);
  valid.vram_required_bytes = 100;
  valid.gpu_count_required = 1;
  EXPECT_TRUE(admission.admit(valid));
}

TEST(P3Batch, PeriodicCheckpointStartsAreStaggeredWithoutRestore) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  PeriodicCheckpointDemand first;
  first.job_id = "first";
  first.src = "A";
  first.dst = "B";
  first.checkpoint_size_bytes = 100;
  first.interval_s = 10;
  first.write_rate_limit_bps = 200;
  auto second = first;
  second.job_id = "second";
  const auto result =
      admission.schedule_checkpoints({first, second}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 2u);
  const auto a = admission.get_reservation("first").value();
  const auto b = admission.get_reservation("second").value();
  EXPECT_EQ(a.eligible_start_ns, kDecisionNs);
  EXPECT_EQ(b.eligible_start_ns, kDecisionNs + 5000000000LL);
  EXPECT_FALSE(a.restore_required);
  EXPECT_FALSE(b.restore_required);
  EXPECT_EQ(a.traffic_class, MigrationTrafficClass::PERIODIC_CHECKPOINT);
}

TEST(P3Batch, UrgentReservationDefersPeriodicCheckpoint) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  PeriodicCheckpointDemand background;
  background.job_id = "background";
  background.src = "A";
  background.dst = "B";
  background.checkpoint_size_bytes = 1000;
  background.interval_s = 10;
  background.write_rate_limit_bps = 1000;
  ASSERT_EQ(
      admission.schedule_checkpoints({background}, kDecisionNs).admitted.size(),
      1u);
  auto urgent = make_entry("urgent", "A", "B", 1000, 10, 0);
  const auto result = admission.admit_batch({urgent}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 1u);
  EXPECT_EQ(result.admitted.front().job_id, "urgent");
  ASSERT_EQ(result.degraded.size(), 1u);
  EXPECT_FALSE(result.degraded.front().recovery_required);
  EXPECT_EQ(result.degraded.front().entry.job_id, "background");
}

TEST(P3Batch, NanosecondNowRejectsExpiredRequests) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  auto entry = make_entry("expired", "A", "B", 100, 8, 0);
  const auto result =
      admission.admit_batch({entry}, kDecisionNs + 8000000000LL);
  EXPECT_TRUE(result.admitted.empty());
  ASSERT_EQ(result.degraded.size(), 1u);
  EXPECT_FALSE(result.degraded.front().reason.empty());
}

TEST(P3Batch, SmallPeriodicPayloadHasNoPlannedOneSecondFloor) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1e6);
  PeriodicCheckpointDemand demand;
  demand.job_id = "small";
  demand.src = "A";
  demand.dst = "B";
  demand.checkpoint_size_bytes = 57;
  demand.interval_s = std::sqrt(16.0 * 57 / (.01 * 100000));
  demand.write_rate_limit_bps = 20000;
  auto result = admission.schedule_checkpoints({demand}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 1u);
  EXPECT_TRUE(result.degraded.empty());
  EXPECT_DOUBLE_EQ(result.admitted.front().allocated_bps, 20000);
  EXPECT_DOUBLE_EQ(result.admitted.front().minimum_useful_transfer_s, 0);
}

TEST(P3Batch, PeriodicGangRetainsCommittedRankAcrossOtherRankCapture) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1e6);
  admission.add_link("C", "D", 1e6);
  PeriodicCheckpointDemand first;
  first.job_id = "gang";
  first.src = "A";
  first.dst = "B";
  first.checkpoint_size_bytes = 57;
  first.bytes_done = 57;
  first.interval_s = 1;
  first.write_rate_limit_bps = 10000;
  first.demand_id = "checkpoint-gang";
  first.gang_size = 2;
  auto second = first;
  second.rank = 1;
  second.src = "C";
  second.dst = "D";
  second.bytes_done = 0;
  auto result = admission.schedule_checkpoints({first, second}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 2u);
  EXPECT_TRUE(result.degraded.empty());
  EXPECT_EQ(admission.get_reservation("gang", 0, "checkpoint-gang")
                ->data_remaining_bytes,
            0u);
  EXPECT_DOUBLE_EQ(
      admission.get_reservation("gang", 0, "checkpoint-gang")->allocated_bps,
      0);
  EXPECT_DOUBLE_EQ(
      admission.get_reservation("gang", 1, "checkpoint-gang")->allocated_bps,
      10000);
}

TEST(P3Batch, ReconcileActualPayloadDoesNotFabricateCompletion) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1e6);
  auto entry = make_entry("job", "A", "B", 101, 8, 0);
  entry.migration_id = "session";
  ASSERT_TRUE(admission.admit(entry));
  admission.update_rank_progress("session", 0, 60);
  ASSERT_TRUE(admission.update_payload("session", 0, 100));
  auto actual = admission.get_reservation("job", 0, "session");
  ASSERT_TRUE(actual);
  EXPECT_EQ(actual->data_total_bytes, 100u);
  EXPECT_EQ(actual->bytes_done, 60u);
  EXPECT_EQ(actual->data_remaining_bytes, 40u);
  ASSERT_TRUE(admission.update_payload("session", 0, 200));
  EXPECT_EQ(
      admission.get_reservation("job", 0, "session")->data_remaining_bytes,
      140u);
  EXPECT_FALSE(admission.update_payload("unknown", 0, 100));
}

TEST(P3Batch, CompletedTransferKeepsItsGangReservationThroughRestoreWindow) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 10000);
  auto entry = make_entry("job", "A", "B", 100, 10, 5);
  entry.migration_id = "final";
  ASSERT_TRUE(admission.admit(entry));
  admission.update_rank_progress("final", 0, 100);
  auto result = admission.admit_batch({}, kDecisionNs + 6000000000LL);
  ASSERT_EQ(result.admitted.size(), 1u);
  EXPECT_TRUE(result.degraded.empty());
  EXPECT_EQ(result.admitted.front().data_remaining_bytes, 0u);
  EXPECT_DOUBLE_EQ(result.admitted.front().allocated_bps, 0);
  EXPECT_TRUE(
      admission.admit_batch({}, kDecisionNs + 10000000000LL).admitted.empty());
}

TEST(P3Batch, HeterogeneousPeriodicRanksHonorSeparateWriteCaps) {
  P3Admission admission(1, 0);
  admission.add_link("A", "B", 1000);
  admission.add_link("C", "D", 1000);
  PeriodicCheckpointDemand first;
  first.job_id = "gang";
  first.src = "A";
  first.dst = "B";
  first.checkpoint_size_bytes = 1;
  first.interval_s = 1;
  first.write_rate_limit_bps = 8;
  first.demand_id = "heterogeneous";
  first.gang_size = 2;
  auto second = first;
  second.src = "C";
  second.dst = "D";
  second.rank = 1;
  second.checkpoint_size_bytes = 9;
  second.write_rate_limit_bps = 72;
  auto result = admission.schedule_checkpoints({first, second}, kDecisionNs);
  ASSERT_EQ(result.admitted.size(), 2u);
  EXPECT_TRUE(result.degraded.empty());
  EXPECT_DOUBLE_EQ(
      admission.get_reservation("gang", 0, "heterogeneous")->allocated_bps, 8);
  EXPECT_DOUBLE_EQ(
      admission.get_reservation("gang", 1, "heterogeneous")->allocated_bps, 72);
}
