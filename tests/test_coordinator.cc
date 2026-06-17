#include "reclaimnet/coordinator.h"
#include "reclaimnet/time.h"
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <limits>

using namespace reclaimnet;
using namespace reclaimnet::control;
namespace {
constexpr int64_t kTraceNs = 1772302390292933888LL;
class CoordinatorTest : public ::testing::Test {
protected:
  std::filesystem::path dir;
  CoordinatorConfig cfg;
  void SetUp() override {
    dir = std::filesystem::temp_directory_path() /
          ("reclaimnet-test-" + std::to_string(mono_ns()));
    std::filesystem::create_directories(dir);
    cfg.sqlite_path = (dir / "runtime.db").string();
    cfg.topology_file.clear();
    cfg.B_ckpt_bps = 1'000'000'000;
    cfg.migration_share = .7;
    cfg.research_min_bps = 50e6;
#if RECLAIMNET_HAVE_JSON
    cfg.topology_file = (dir / "topology.json").string();
    std::ofstream topology(cfg.topology_file);
    topology << R"({"directed":false,"links":[
      {"src":"A","dst":"switch","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"},
      {"src":"B","dst":"switch","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"},
      {"src":"C","dst":"switch","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraBuilding"}]})";
#endif
  }
  void TearDown() override { std::filesystem::remove_all(dir); }
  void register_nodes(Coordinator &coordinator) {
    for (const auto *id : {"A", "B", "C"}) {
      NodeRecord node{};
      node.node_id = id;
      node.state = NodeState::ONLINE;
      node.address = std::string(id) + ":50052";
      node.link_capacity_bps = 1'000'000'000;
      node.total_vram_bytes = 2'000'000'000;
      node.registered_at_ns = kTraceNs;
      node.last_heartbeat_ns = kTraceNs;
      node.telemetry.vram_free_bytes = 2'000'000'000;
      node.telemetry.updated_ns = kTraceNs;
      ASSERT_TRUE(coordinator.register_node(node));
    }
  }
  JobRecord job(const std::string &id = "job") {
    JobRecord j{};
    j.job_id = id;
    j.node_id = "A";
    j.state = JobState::RUNNING;
    j.lambda_i = 1e-5;
    j.checkpoint_size = 1'000'000;
    j.vram_bytes = 1'000'000'000;
    j.submit_time_ns = kTraceNs;
    j.last_update_ns = kTraceNs;
    return j;
  }
};
}

TEST_F(CoordinatorTest, EpochKeepsHazardAndPublishesCheckpointPlan) {
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  ASSERT_TRUE(coordinator.on_hazard_sample("A", 1e-5, kTraceNs));
  coordinator.run_one_epoch(kTraceNs);
  coordinator.run_one_epoch(kTraceNs + 30'000'000'000LL);
  auto record = coordinator.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_DOUBLE_EQ(record->lambda_i, 1e-5);
  EXPECT_DOUBLE_EQ(record->checkpoint_allocated_bps, 1e9);
  EXPECT_DOUBLE_EQ(record->checkpoint_write_rate_bps, 2e8);
  EXPECT_TRUE(record->checkpoint_constraints_feasible);
  auto commands = coordinator.checkpoint_commands("A");
  ASSERT_EQ(commands.size(), 1u);
  EXPECT_NEAR(commands[0].interval_s, std::sqrt(16e6 / (1e-5 * 1e9)), 1e-8);
  EXPECT_TRUE(commands[0].feasible);
}

TEST_F(CoordinatorTest, TraceHazardSampleControlsTheNextEpoch) {
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  ASSERT_TRUE(coordinator.on_hazard_sample("A", 1e-4, kTraceNs));
  ASSERT_TRUE(coordinator.on_hazard_sample("A", 1.0, kTraceNs - 1));
  coordinator.run_one_epoch(kTraceNs);
  auto record = coordinator.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_NEAR(record->checkpoint_interval_s, std::sqrt(16e6 / (1e-4 * 1e9)),
              1e-8);
  EXPECT_DOUBLE_EQ(record->lambda_i, 1e-5);
  EXPECT_FALSE(coordinator.on_hazard_sample(
      "A", std::numeric_limits<double>::quiet_NaN(), kTraceNs));
}

TEST_F(CoordinatorTest,
       OnlineHazardUsesObservedZeroEventWindowInsteadOfPermanentPrior) {
  cfg.heartbeat_timeout_s = 10000;
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  const auto stamp = kTraceNs + 3'600'000'000'000LL;
  coordinator.run_one_epoch(stamp);
  const auto upper = coordinator.hazard().confidence_upper_bound("A", stamp);
  ASSERT_TRUE(upper);
  EXPECT_DOUBLE_EQ(coordinator.hazard().estimate_at("A", stamp).value(), 0);
  EXPECT_NEAR(coordinator.jobs().get("job")->checkpoint_interval_s,
              std::sqrt(16e6 / (*upper * 1e9)), 1e-9);
  EXPECT_FALSE(
      coordinator.hazard().summary_at("A", kTraceNs)->confidence_available);
  EXPECT_TRUE(
      coordinator.hazard().summary_at("A", stamp)->confidence_available);
}

TEST_F(CoordinatorTest, StopWakesEpochWaitImmediately) {
  cfg.epoch_seconds = 30;
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start());
  auto before = std::chrono::steady_clock::now();
  coordinator.stop();
  EXPECT_LT(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - before)
          .count(),
      1.0);
}

TEST_F(CoordinatorTest, RepeatedStopDoesNotFinalizeRemovedTraceDirectory) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON trace support disabled";
#else
  cfg.trace_dir = (dir / "trace").string();
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  coordinator.stop();
  std::filesystem::remove_all(cfg.trace_dir);
  EXPECT_NO_THROW(coordinator.stop());
#endif
}

TEST_F(CoordinatorTest, ScheduledReclaimDoesNotChangeEmergencyHazard) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  ASSERT_TRUE(coordinator.on_hazard_sample("A", 1e-5, kTraceNs - 1));
  ASSERT_TRUE(coordinator.on_reclaim_signal("A", "job", 60,
                                            ReclaimType::SCHEDULED, kTraceNs));
  EXPECT_DOUBLE_EQ(coordinator.hazard().lambda("A"), 1e-5);
#endif
}

TEST_F(CoordinatorTest, MigrationHasPositiveRateAndIdempotentRetry) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  auto first = coordinator.on_reclaim_signal("A", "job", 60,
                                             ReclaimType::SCHEDULED, kTraceNs);
  ASSERT_TRUE(first);
  EXPECT_GT(first.value().allocated_bps, 0);
  EXPECT_EQ(first.value().deadline_ns, kTraceNs + 60'000'000'000LL);
  auto retry = coordinator.on_reclaim_signal(
      "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs + 1);
  ASSERT_TRUE(retry);
  EXPECT_DOUBLE_EQ(retry.value().allocated_bps, first.value().allocated_bps);
  EXPECT_EQ(retry.value().deadline_ns, first.value().deadline_ns);
  coordinator.run_one_epoch(kTraceNs + 30'000'000'000LL);
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::MIGRATING);
  coordinator.run_one_epoch(kTraceNs + 60'000'000'000LL);
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::SUSPENDED);
#endif
}

TEST_F(CoordinatorTest, CompletedMigrationMovesJobAndPersistsIt) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite are needed for migration persistence";
#else
  std::string destination;
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    ASSERT_TRUE(coordinator.register_job(job()));
    auto migration = coordinator.on_reclaim_signal(
        "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs);
    ASSERT_TRUE(migration);
    destination = migration.value().dst_node;
    EXPECT_FALSE(coordinator.on_migration_progress("job", 500'000, true));
    EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::MIGRATING);
    ASSERT_TRUE(coordinator.on_migration_progress(
        "job", 1'000'000, true, "", 0, false, kTraceNs + 1'000'000'000LL));
    auto moved = coordinator.jobs().get("job");
    ASSERT_TRUE(moved);
    EXPECT_EQ(moved->node_id, destination);
    EXPECT_TRUE(moved->migration_dst.empty());
    EXPECT_EQ(moved->state, JobState::RUNNING);
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  auto record = restored.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_EQ(record->node_id, destination);
  EXPECT_EQ(record->state, JobState::RUNNING);
  EXPECT_DOUBLE_EQ(record->lambda_i, 1e-5);
#endif
}

TEST_F(CoordinatorTest, RestartSuspendsInterruptedMigration) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite are needed for migration persistence";
#else
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    ASSERT_TRUE(coordinator.register_job(job()));
    ASSERT_TRUE(coordinator.on_reclaim_signal(
        "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs));
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  auto record = restored.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_EQ(record->state, JobState::SUSPENDED);
  EXPECT_TRUE(record->migration_dst.empty());
#endif
}

TEST_F(CoordinatorTest, InvalidOrEmergencyWithoutGraceDoesNotAdmit) {
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  EXPECT_FALSE(coordinator.on_reclaim_signal("B", "job", 60,
                                             ReclaimType::SCHEDULED, kTraceNs));
  EXPECT_FALSE(coordinator.on_reclaim_signal("A", "job", 0,
                                             ReclaimType::EMERGENCY, kTraceNs));
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::SUSPENDED);
}

TEST(Persistence, RuntimeExtensionsKeepTraceV3CoreTablesCompatible) {
#if !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "SQLite disabled";
#else
  PersistenceStore store(":memory:");
  ASSERT_TRUE(store.init_schema());
  JobRecord j{};
  j.job_id = "job'quoted";
  j.node_id = "A";
  j.state = JobState::RUNNING;
  j.lambda_i = 1e-5;
  j.checkpoint_size = 239244449;
  j.submit_time_ns = kTraceNs;
  j.checkpoint_interval_s = 3210.692863638208;
  j.checkpoint_allocated_bps = 370315888;
  j.checkpoint_write_rate_bps = 74063177.6;
  j.checkpoint_constraints_feasible = true;
  j.write_cap_bps = 370315888;
  ASSERT_TRUE(store.upsert_job(j));
  auto rows = store.load_all_jobs();
  ASSERT_TRUE(rows);
  ASSERT_EQ(rows.value().size(), 1u);
  EXPECT_EQ(rows.value()[0].submit_time_ns, kTraceNs);
  EXPECT_DOUBLE_EQ(rows.value()[0].checkpoint_write_rate_bps,
                   j.checkpoint_write_rate_bps);
  EXPECT_DOUBLE_EQ(rows.value()[0].lambda_i, j.lambda_i);
  ASSERT_TRUE(store.delete_job(j.job_id));
  EXPECT_TRUE(store.load_all_jobs().value().empty());
#endif
}

TEST_F(CoordinatorTest, DefaultJobUsesConfiguredLossBudget) {
  cfg.checkpoint_loss_budget_s = 10;
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  coordinator.run_one_epoch(kTraceNs);
  auto record = coordinator.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_DOUBLE_EQ(record->checkpoint_interval_s, 20);
}

TEST_F(CoordinatorTest, SameObjectRestartDoesNotKeepOldReservations) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite are needed for migration persistence";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  ASSERT_TRUE(coordinator.on_reclaim_signal("A", "job", 60,
                                            ReclaimType::SCHEDULED, kTraceNs));
  coordinator.stop();
  ASSERT_TRUE(coordinator.start(false));
  auto record = coordinator.jobs().get("job");
  ASSERT_TRUE(record);
  EXPECT_EQ(record->state, JobState::SUSPENDED);
  EXPECT_TRUE(record->migration_dst.empty());
#endif
}

TEST_F(CoordinatorTest, NodeLevelEmergencyTraceEventAllowsZeroNotice) {
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.on_reclaim_event("A", "", 0, ReclaimType::EMERGENCY,
                                           kTraceNs));
  auto estimate = coordinator.hazard().snapshot().at("A");
  EXPECT_EQ(estimate.last_event_ns, kTraceNs);
  EXPECT_GT(coordinator.hazard().estimate_at("A", kTraceNs).value_or(0), 0);
  EXPECT_FALSE(coordinator.on_reclaim_event("A", "", 0, ReclaimType::SCHEDULED,
                                            kTraceNs));
  EXPECT_FALSE(coordinator.on_reclaim_event("unknown", "", 0,
                                            ReclaimType::EMERGENCY, kTraceNs));
}

TEST_F(CoordinatorTest,
       DistributedP1SumsComponentHazardsAndSplitsLogicalWriteBudget) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  cfg.B_ckpt_bps = 471185102;
  std::ofstream topology(cfg.topology_file);
  topology << R"({"directed":false,"links":[)";
  for (int i = 0; i < 8; ++i) {
    if (i)
      topology << ",";
    topology << "{\"src\":\"N" << i
             << "\",\"dst\":\"switch\",\"bandwidth_bps\":1000000000,\"latency_"
                "us\":1,\"level\":\"IntraRack\"}";
  }
  topology << "]}";
  topology.close();
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  for (int i = 0; i < 8; ++i) {
    NodeRecord node;
    node.node_id = "N" + std::to_string(i);
    node.state = NodeState::ONLINE;
    node.address = node.node_id;
    node.total_vram_bytes = 2'000'000'000;
    node.telemetry.vram_free_bytes = node.total_vram_bytes;
    node.link_capacity_bps = 1e9;
    node.registered_at_ns = kTraceNs;
    node.last_heartbeat_ns = kTraceNs;
    ASSERT_TRUE(coordinator.register_node(node));
  }
  auto j = job("job-083");
  j.node_id = "N0";
  j.rank_nodes = {"N0", "N1", "N2", "N3"};
  j.rank_count = 4;
  j.checkpoint_size = 7076779740ULL;
  j.write_cap_bps = 1148766268.0;
  ASSERT_TRUE(coordinator.register_job(j));
  const double components[] = {2.3239432422986133e-6, 2.721181906450274e-6,
                               2.339217798080195e-6, 2.711034100107159e-6};
  for (int i = 0; i < 4; ++i)
    ASSERT_TRUE(coordinator.on_hazard_sample("N" + std::to_string(i),
                                             components[i], kTraceNs));
  coordinator.run_one_epoch(kTraceNs);
  auto record = coordinator.jobs().get(j.job_id);
  ASSERT_TRUE(record);
  EXPECT_NEAR(record->checkpoint_interval_s,
              std::sqrt(16.0 * j.checkpoint_size /
                        (1.009537704693624e-5 * cfg.B_ckpt_bps)),
              1e-8);
  EXPECT_NEAR(record->checkpoint_write_rate_bps, .2 * cfg.B_ckpt_bps, 1e-6);
  double written = 0;
  for (int i = 0; i < 4; ++i) {
    auto commands = coordinator.checkpoint_commands("N" + std::to_string(i));
    ASSERT_EQ(commands.size(), 1u);
    ASSERT_TRUE(commands.front().feasible);
    EXPECT_EQ(commands.front().rank, static_cast<uint32_t>(i));
    EXPECT_NEAR(commands.front().write_rate_limit_bps, .2 * cfg.B_ckpt_bps / 4,
                1e-6);
    written += commands.front().write_rate_limit_bps;
  }
  EXPECT_NEAR(written, record->checkpoint_write_rate_bps, 1e-6);
#endif
}

TEST_F(CoordinatorTest,
       EmergencyGangStopsSurvivingSourcesBeforeRecoveryAndStartsTogether) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  NodeRecord d = *coordinator.nodes().get("C");
  d.node_id = "D";
  d.address = "D";
  ASSERT_TRUE(coordinator.register_node(d));
  auto j = job();
  j.rank_nodes = {"A", "B"};
  j.rank_count = 2;
  ASSERT_TRUE(coordinator.register_job(j));
  for (uint32_t rank = 0; rank < 2; ++rank) {
    ReplicaCheckpoint checkpoint;
    checkpoint.job_id = j.job_id;
    checkpoint.checkpoint_id = "committed-gang";
    checkpoint.sequence = 1;
    checkpoint.size_bytes = 500000;
    checkpoint.completed_at_ns = kTraceNs + 10;
    checkpoint.source_node_ids = {"A", "B"};
    checkpoint.replica_node_id = rank ? "D" : "C";
    checkpoint.path = "/tmp/checkpoint-rank-" + std::to_string(rank);
    checkpoint.checksum_sha256 = std::string(64, 'a');
    checkpoint.rank = rank;
    checkpoint.gang_size = 2;
    ASSERT_TRUE(coordinator.commit_checkpoint(checkpoint));
  }
  auto recovery = coordinator.on_reclaim_signal(
      "A", j.job_id, 60, ReclaimType::EMERGENCY, kTraceNs + 20);
  ASSERT_TRUE(recovery);
  EXPECT_TRUE(recovery.value().recovery);
  EXPECT_TRUE(recovery.value().recoveries.empty());
  EXPECT_TRUE(coordinator.stop_jobs("A").empty());
  auto stops = coordinator.stop_jobs("B");
  ASSERT_EQ(stops.size(), 1u);
  EXPECT_TRUE(coordinator.recovery_commands("C").empty());
  EXPECT_TRUE(coordinator.recovery_commands("D").empty());
  EXPECT_FALSE(coordinator.recovery_complete("C", j.job_id, "committed-gang", 0,
                                             kTraceNs + 30));
  EXPECT_FALSE(
      coordinator.source_stopped("B", j.job_id, "wrong", kTraceNs + 30));
  ASSERT_TRUE(coordinator.source_stopped("B", j.job_id, stops.front().second,
                                         kTraceNs + 30));
  ASSERT_TRUE(coordinator.source_stopped("B", j.job_id, stops.front().second,
                                         kTraceNs + 31));
  EXPECT_TRUE(coordinator.stop_jobs("B").empty());
  ASSERT_EQ(coordinator.recovery_commands("C").size(), 1u);
  ASSERT_EQ(coordinator.recovery_commands("D").size(), 1u);
  ASSERT_TRUE(coordinator.recovery_complete("C", j.job_id, "committed-gang", 0,
                                            kTraceNs + 40));
  EXPECT_EQ(coordinator.jobs().get(j.job_id)->state, JobState::SUSPENDED);
  EXPECT_TRUE(coordinator.start_jobs("C").empty());
  ASSERT_TRUE(coordinator.recovery_complete("D", j.job_id, "committed-gang", 1,
                                            kTraceNs + 41));
  EXPECT_EQ(coordinator.jobs().get(j.job_id)->state, JobState::RUNNING);
  ASSERT_EQ(coordinator.start_jobs("C").size(), 1u);
  ASSERT_EQ(coordinator.start_jobs("D").size(), 1u);
  EXPECT_EQ(coordinator.start_jobs("C").front().second,
            coordinator.start_jobs("D").front().second);
  EXPECT_EQ(coordinator.jobs().get(j.job_id)->rank_nodes,
            (std::vector<NodeId>{"C", "D"}));
  EXPECT_TRUE(coordinator.recovery_complete("C", j.job_id, "committed-gang", 0,
                                            kTraceNs + 42));
  NodeTelemetry heartbeat = d.telemetry;
  heartbeat.updated_ns = kTraceNs + 50;
  coordinator.on_heartbeat("A", heartbeat);
  EXPECT_EQ(coordinator.nodes().get("A")->state, NodeState::OFFLINE);
#endif
}

TEST_F(CoordinatorTest,
       BatchPlacementUsesShadowGpuResourcesAndKeepsRequestOrder) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job("later")));
  ASSERT_TRUE(coordinator.register_job(job("earlier")));
  auto results = coordinator.on_reclaim_batch(
      {{"A", "later", 61, ReclaimType::SCHEDULED, kTraceNs},
       {"A", "earlier", 60, ReclaimType::SCHEDULED, kTraceNs}});
  ASSERT_EQ(results.size(), 2u);
  ASSERT_TRUE(results[0]);
  ASSERT_TRUE(results[1]);
  EXPECT_EQ(results[0].value().job_id, "later");
  EXPECT_EQ(results[1].value().job_id, "earlier");
  EXPECT_NE(results[0].value().dst_node, results[1].value().dst_node);
  EXPECT_EQ(results[1].value().dst_node, "B");
  EXPECT_EQ(coordinator.nodes().get("B")->gpu_free_count, 1u);
  EXPECT_EQ(coordinator.nodes().get("C")->gpu_free_count, 1u);
  EXPECT_EQ(coordinator.jobs().get("earlier")->state, JobState::MIGRATING);
  EXPECT_EQ(coordinator.jobs().get("later")->state, JobState::MIGRATING);
#endif
}

TEST_F(CoordinatorTest, BatchDuplicateJobSharesOneMigrationSession) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  auto results = coordinator.on_reclaim_batch(
      {{"A", "job", 60, ReclaimType::SCHEDULED, kTraceNs},
       {"A", "job", 60, ReclaimType::SCHEDULED, kTraceNs + 1}});
  ASSERT_EQ(results.size(), 2u);
  ASSERT_TRUE(results[0]);
  ASSERT_TRUE(results[1]);
  EXPECT_EQ(results[0].value().migration_id, results[1].value().migration_id);
  EXPECT_EQ(results[0].value().dst_node, results[1].value().dst_node);
  EXPECT_DOUBLE_EQ(results[0].value().allocated_bps,
                   results[1].value().allocated_bps);
  EXPECT_EQ(coordinator.rate_updates("A").size(), 1u);
#endif
}

TEST_F(CoordinatorTest,
       FinalWaterFillStillHonorsSharedPathsAfterAnotherJobCaps) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  cfg.B_ckpt_bps = 3'000'000'000;
  std::ofstream topology(cfg.topology_file);
  topology << R"({"directed":false,"links":[
    {"src":"A","dst":"X","bandwidth_bps":2000000000,"latency_us":1,"level":"IntraRack"},
    {"src":"C","dst":"X","bandwidth_bps":2000000000,"latency_us":1,"level":"IntraRack"},
    {"src":"X","dst":"Y","bandwidth_bps":1000000000,"latency_us":1,"level":"CrossBuilding"},
    {"src":"B","dst":"Y","bandwidth_bps":2000000000,"latency_us":1,"level":"IntraRack"},
    {"src":"W","dst":"Z","bandwidth_bps":2000000000,"latency_us":1,"level":"IntraRack"}]})";
  topology.close();
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  for (const auto *id : {"A", "B", "C", "W", "Z"}) {
    NodeRecord node;
    node.node_id = id;
    node.state = NodeState::ONLINE;
    node.address = id;
    node.total_vram_bytes = 2'000'000'000;
    node.telemetry.vram_free_bytes = node.total_vram_bytes;
    node.link_capacity_bps = 2'000'000'000;
    node.registered_at_ns = kTraceNs;
    node.last_heartbeat_ns = kTraceNs;
    node.gpu_free_count =
        std::string(id) == "B" || std::string(id) == "Z" ? 1 : 0;
    ASSERT_TRUE(coordinator.register_node(node));
  }
  for (const auto *id : {"A", "C", "W"}) {
    auto j = job(id);
    j.node_id = id;
    if (std::string(id) == "W")
      j.write_cap_bps = 100'000'000;
    ASSERT_TRUE(coordinator.register_job(j));
    ASSERT_TRUE(coordinator.on_hazard_sample(id, 1e-5, kTraceNs));
  }
  coordinator.run_one_epoch(kTraceNs);
  const auto a = coordinator.jobs().get("A")->checkpoint_allocated_bps;
  const auto c = coordinator.jobs().get("C")->checkpoint_allocated_bps;
  const auto w = coordinator.jobs().get("W")->checkpoint_allocated_bps;
  EXPECT_NEAR(a, 500'000'000, 1e-3);
  EXPECT_NEAR(c, 500'000'000, 1e-3);
  EXPECT_LE(a + c, 1'000'000'000.002);
  EXPECT_DOUBLE_EQ(w, 100'000'000);
#endif
}

TEST_F(CoordinatorTest,
       FinalActualPayloadUpdatesRateAndRejectsDeadlineInfeasibleGrowth) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job()));
  auto command = coordinator.on_reclaim_signal(
      "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs);
  ASSERT_TRUE(command);
  auto actual = coordinator.prepare_final_payload(
      "job", command.value().migration_id, 0, 2'000'000, kTraceNs + 1);
  ASSERT_TRUE(actual);
  EXPECT_EQ(actual.value().data_total_bytes, 2'000'000u);
  EXPECT_EQ(actual.value().bytes_done, 0u);
  EXPECT_EQ(actual.value().data_remaining_bytes, 2'000'000u);
  EXPECT_GT(actual.value().allocation_version,
            command.value().ranks.front().allocation_version);
  EXPECT_GT(actual.value().allocated_bps, command.value().allocated_bps);
  EXPECT_FALSE(
      coordinator.prepare_final_payload("job", command.value().migration_id, 0,
                                        1'000'000'000'000ULL, kTraceNs + 2));
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::SUSPENDED);
#endif
}

TEST_F(CoordinatorTest, FullGangCommitCalibratesAndPersistsActualRankPayloads) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite required";
#else
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    auto j = job();
    j.rank_count = 2;
    j.rank_nodes = {"A", "B"};
    ASSERT_TRUE(coordinator.register_job(j));
    for (uint32_t rank = 0; rank < 2; ++rank) {
      ReplicaCheckpoint copy;
      copy.job_id = j.job_id;
      copy.checkpoint_id = "measured";
      copy.sequence = 1;
      copy.rank = rank;
      copy.gang_size = 2;
      copy.size_bytes = rank ? 9 : 3;
      copy.completed_at_ns = kTraceNs + rank + 1;
      copy.source_node_ids = j.rank_nodes;
      copy.replica_node_id = "C";
      copy.path = "/tmp/measured-" + std::to_string(rank);
      copy.checksum_sha256 = std::string(64, 'a');
      ASSERT_TRUE(coordinator.commit_checkpoint(copy));
      if (!rank)
        EXPECT_EQ(coordinator.jobs().get(j.job_id)->checkpoint_size,
                  j.checkpoint_size);
    }
    EXPECT_EQ(coordinator.jobs().get(j.job_id)->checkpoint_size, 12u);
    EXPECT_EQ(coordinator.jobs().get(j.job_id)->rank_checkpoint_sizes,
              (std::vector<uint64_t>{3, 9}));
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  EXPECT_EQ(restored.jobs().get("job")->checkpoint_size, 12u);
  EXPECT_EQ(restored.jobs().get("job")->rank_checkpoint_sizes,
            (std::vector<uint64_t>{3, 9}));
#endif
}

TEST_F(CoordinatorTest, RestartReplaysCausalHazardSamplesAndEmergencyHistory) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite required";
#else
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    ASSERT_TRUE(coordinator.on_hazard_sample("B", 1e-4, kTraceNs + 1));
    ASSERT_TRUE(coordinator.on_hazard_sample("B", 1.0, kTraceNs + 100));
    ASSERT_TRUE(coordinator.on_reclaim_event("A", "", 0, ReclaimType::EMERGENCY,
                                             kTraceNs + 2));
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  EXPECT_DOUBLE_EQ(restored.hazard().estimate_at("B", kTraceNs + 50).value(),
                   1e-4);
  EXPECT_FALSE(restored.hazard().confidence_upper_bound("B", kTraceNs + 50));
  EXPECT_EQ(restored.hazard().summary_at("A", kTraceNs + 50)->event_count, 1u);
  EXPECT_TRUE(restored.hazard().confidence_upper_bound("A", kTraceNs + 50));
#endif
}

TEST_F(CoordinatorTest,
       RestoredRankHeartbeatDoesNotDoubleCountItsGpuReservation) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  register_nodes(coordinator);
  ASSERT_TRUE(coordinator.register_job(job(), "{}"));
  auto command = coordinator.on_reclaim_signal(
      "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs);
  ASSERT_TRUE(command);
  ReplicaCheckpoint copy;
  copy.job_id = "job";
  copy.checkpoint_id = command.value().migration_id;
  copy.sequence = 1;
  copy.size_bytes = 1'000'000;
  copy.completed_at_ns = kTraceNs + 10;
  copy.source_node_ids = {"A"};
  copy.replica_node_id = command.value().dst_node;
  copy.path = "/tmp/final-restored";
  copy.checksum_sha256 = std::string(64, 'a');
  ASSERT_TRUE(
      coordinator.commit_checkpoint(copy, command.value().migration_id, true));
  NodeTelemetry telemetry;
  telemetry.updated_ns = kTraceNs + 20;
  telemetry.gpu_free_count = 0;
  telemetry.vram_free_bytes = 1'000'000'000;
  coordinator.on_heartbeat(copy.replica_node_id, telemetry);
  coordinator.run_one_epoch(kTraceNs + 21);
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::MIGRATING);
  ASSERT_TRUE(coordinator.on_migration_progress("job", 1'000'000, true,
                                                command.value().migration_id, 0,
                                                false, kTraceNs + 30));
  EXPECT_EQ(coordinator.jobs().get("job")->state, JobState::RUNNING);
#endif
}

TEST_F(CoordinatorTest, RestartPreservesCompletedMigrationAckAndResumeBarrier) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite required";
#else
  std::string destination, migration;
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    ASSERT_TRUE(coordinator.register_job(job()));
    auto command = coordinator.on_reclaim_signal(
        "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs);
    ASSERT_TRUE(command);
    destination = command.value().dst_node;
    migration = command.value().migration_id;
    ASSERT_TRUE(coordinator.on_migration_progress(
        "job", 1'000'000, true, migration, 0, false, kTraceNs + 100));
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  ASSERT_EQ(restored.start_jobs(destination).size(), 1u);
  EXPECT_EQ(restored.start_jobs(destination).front(),
            std::make_pair(std::string("job"), migration));
  EXPECT_TRUE(restored.on_migration_progress("job", 1'000'000, true, migration,
                                             0, false, kTraceNs + 101));
  PersistenceStore inspect(cfg.sqlite_path);
  ASSERT_TRUE(inspect.init_schema());
  auto costs = inspect.load_runtime("migration_cost");
  ASSERT_TRUE(costs);
  ASSERT_EQ(costs.value().size(), 1u);
  EXPECT_NE(costs.value().front().second.find("duration_s"), std::string::npos);
#endif
}

TEST_F(CoordinatorTest,
       InterruptedMigrationReusesItsAlreadyRestoredOccupiedTarget) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite required";
#else
  std::string destination, migration;
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    ASSERT_TRUE(coordinator.register_job(job(), "{}"));
    auto command = coordinator.on_reclaim_signal(
        "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs);
    ASSERT_TRUE(command);
    destination = command.value().dst_node;
    migration = command.value().migration_id;
    ReplicaCheckpoint copy;
    copy.job_id = "job";
    copy.checkpoint_id = migration;
    copy.sequence = 1;
    copy.size_bytes = 1'000'000;
    copy.completed_at_ns = kTraceNs + 10;
    copy.source_node_ids = {"A"};
    copy.replica_node_id = destination;
    copy.path = "/tmp/restored-before-controller-crash";
    copy.checksum_sha256 = std::string(64, 'a');
    ASSERT_TRUE(coordinator.commit_checkpoint(copy, migration, true));
    NodeTelemetry telemetry;
    telemetry.updated_ns = kTraceNs + 20;
    telemetry.gpu_free_count = 0;
    telemetry.vram_free_bytes = 0;
    coordinator.on_heartbeat(destination, telemetry);
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  EXPECT_EQ(restored.jobs().get("job")->state, JobState::SUSPENDED);
  restored.run_one_epoch(kTraceNs + 21);
  auto stops = restored.stop_jobs("A");
  ASSERT_EQ(stops.size(), 1u);
  EXPECT_TRUE(restored.recovery_commands(destination).empty());
  ASSERT_TRUE(
      restored.source_stopped("A", "job", stops.front().second, kTraceNs + 22));
  auto plans = restored.recovery_commands(destination);
  ASSERT_EQ(plans.size(), 1u);
  EXPECT_EQ(plans.front().checkpoint_id, migration);
  ASSERT_TRUE(restored.recovery_complete(destination, "job", migration, 0,
                                         kTraceNs + 30));
  EXPECT_EQ(restored.jobs().get("job")->state, JobState::RUNNING);
  EXPECT_EQ(restored.start_jobs(destination).size(), 1u);
#endif
}

TEST_F(CoordinatorTest, RestartCompletesRecoveryWithAnAlreadyRestoredGangRank) {
#if !RECLAIMNET_HAVE_JSON || !RECLAIMNET_HAVE_SQLITE
  GTEST_SKIP() << "JSON and SQLite required";
#else
  {
    Coordinator coordinator(cfg);
    ASSERT_TRUE(coordinator.start(false));
    register_nodes(coordinator);
    auto d = *coordinator.nodes().get("C");
    d.node_id = "D";
    ASSERT_TRUE(coordinator.register_node(d));
    auto j = job();
    j.rank_nodes = {"A", "B"};
    j.rank_count = 2;
    ASSERT_TRUE(coordinator.register_job(j));
    for (uint32_t rank = 0; rank < 2; ++rank) {
      ReplicaCheckpoint copy;
      copy.job_id = "job";
      copy.checkpoint_id = "surviving";
      copy.rank = rank;
      copy.gang_size = 2;
      copy.sequence = 1;
      copy.size_bytes = 5;
      copy.completed_at_ns = kTraceNs + 10;
      copy.replica_node_id = rank ? "D" : "C";
      copy.source_node_ids = j.rank_nodes;
      copy.path = "/tmp/surviving-" + std::to_string(rank);
      copy.checksum_sha256 = std::string(64, 'a');
      ASSERT_TRUE(coordinator.commit_checkpoint(copy));
    }
    ASSERT_TRUE(coordinator.on_reclaim_signal(
        "A", "job", 0, ReclaimType::EMERGENCY, kTraceNs + 20));
    auto stops = coordinator.stop_jobs("B");
    ASSERT_EQ(stops.size(), 1u);
    ASSERT_TRUE(coordinator.source_stopped("B", "job", stops.front().second,
                                           kTraceNs + 21));
    ASSERT_TRUE(coordinator.recovery_complete("C", "job", "surviving", 0,
                                              kTraceNs + 30));
    NodeTelemetry telemetry;
    telemetry.updated_ns = kTraceNs + 31;
    telemetry.gpu_free_count = 0;
    telemetry.vram_free_bytes = 0;
    coordinator.on_heartbeat("C", telemetry);
  }
  Coordinator restored(cfg);
  ASSERT_TRUE(restored.start(false));
  restored.run_one_epoch(kTraceNs + 32);
  auto stops = restored.stop_jobs("B");
  ASSERT_EQ(stops.size(), 1u);
  ASSERT_TRUE(
      restored.source_stopped("B", "job", stops.front().second, kTraceNs + 33));
  ASSERT_EQ(restored.recovery_commands("C").size(), 1u);
  ASSERT_EQ(restored.recovery_commands("D").size(), 1u);
  ASSERT_TRUE(
      restored.recovery_complete("D", "job", "surviving", 1, kTraceNs + 34));
  EXPECT_EQ(restored.jobs().get("job")->state, JobState::RUNNING);
  EXPECT_EQ(restored.start_jobs("C").size(), 1u);
  EXPECT_EQ(restored.start_jobs("D").size(), 1u);
#endif
}

TEST_F(CoordinatorTest,
       GangVramRemainderControlsPlacementReservationAndRecovery) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  std::ofstream topology(cfg.topology_file);
  topology
      << R"({"links":[{"src":"A","dst":"sw","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"},)"
         R"({"src":"B","dst":"sw","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"},)"
         R"({"src":"C","dst":"sw","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"},)"
         R"({"src":"Z","dst":"sw","bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"}]})";
  topology.close();
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  for (const auto *id : {"A", "B", "C", "Z"}) {
    NodeRecord node;
    node.node_id = id;
    node.state = NodeState::ONLINE;
    node.address = id;
    node.total_vram_bytes = std::string(id) == "C"   ? 8
                            : std::string(id) == "Z" ? 9
                                                     : 17;
    node.telemetry.vram_free_bytes = node.total_vram_bytes;
    node.link_capacity_bps = 1'000'000'000;
    node.registered_at_ns = kTraceNs;
    node.last_heartbeat_ns = kTraceNs;
    ASSERT_TRUE(coordinator.register_node(node));
  }
  auto j = job();
  j.rank_nodes = {"A", "B"};
  j.rank_count = 2;
  j.vram_bytes = 17;
  j.checkpoint_size = 17;
  ASSERT_TRUE(coordinator.register_job(j));
  coordinator.run_one_epoch(kTraceNs);
  ASSERT_EQ(coordinator.checkpoint_commands("A").size(), 1u);
  ASSERT_EQ(coordinator.checkpoint_commands("B").size(), 1u);
  EXPECT_EQ(coordinator.checkpoint_commands("A").front().replica_node_id, "Z");
  EXPECT_EQ(coordinator.checkpoint_commands("B").front().replica_node_id, "C");
  auto planned = coordinator.on_reclaim_signal(
      "A", "job", 60, ReclaimType::SCHEDULED, kTraceNs + 1);
  ASSERT_TRUE(planned);
  ASSERT_EQ(planned.value().ranks.size(), 2u);
  EXPECT_EQ(planned.value().ranks[0].destination_node, "Z");
  EXPECT_EQ(planned.value().ranks[1].destination_node, "C");
  EXPECT_EQ(coordinator.rate_updates("A").front().vram_required_bytes, 9u);
  EXPECT_EQ(coordinator.rate_updates("B").front().vram_required_bytes, 8u);
  for (uint32_t rank = 0; rank < 2; ++rank) {
    ReplicaCheckpoint copy;
    copy.job_id = "job";
    copy.checkpoint_id = "snapshot";
    copy.sequence = 1;
    copy.rank = rank;
    copy.gang_size = 2;
    copy.replica_node_id = rank ? "C" : "Z";
    copy.source_node_ids = j.rank_nodes;
    copy.size_bytes = rank ? 8 : 9;
    copy.completed_at_ns = kTraceNs + 10;
    copy.path = "/tmp/rank-vram-" + std::to_string(rank);
    copy.checksum_sha256 = std::string(64, 'a');
    ASSERT_TRUE(coordinator.commit_checkpoint(copy));
  }
  auto recovery = coordinator.on_reclaim_signal(
      "B", "job", 0, ReclaimType::EMERGENCY, kTraceNs + 20);
  ASSERT_TRUE(recovery);
  auto stops = coordinator.stop_jobs("A");
  ASSERT_EQ(stops.size(), 1u);
  ASSERT_TRUE(coordinator.source_stopped("A", "job", stops.front().second,
                                         kTraceNs + 21));
  ASSERT_EQ(coordinator.recovery_commands("Z").size(), 1u);
  ASSERT_EQ(coordinator.recovery_commands("C").size(), 1u);
  EXPECT_EQ(coordinator.recovery_commands("Z").front().rank, 0u);
  EXPECT_EQ(coordinator.recovery_commands("C").front().rank, 1u);
#endif
}

TEST(CheckpointCatalogEligibility,
     RejectsIncompatibleRankAndFallsBackToCompleteSnapshot) {
  CheckpointCatalog catalog;
  for (uint32_t sequence = 1; sequence <= 2; ++sequence)
    for (uint32_t rank = 0; rank < 2; ++rank) {
      ReplicaCheckpoint copy;
      copy.job_id = "job";
      copy.checkpoint_id = "snapshot-" + std::to_string(sequence);
      copy.sequence = sequence;
      copy.rank = rank;
      copy.gang_size = 2;
      copy.size_bytes = 1;
      copy.completed_at_ns = kTraceNs + sequence;
      copy.replica_node_id =
          (sequence == 1 ? rank == 0 : rank == 1) ? "large" : "small";
      copy.path = "/tmp/catalog-rank-" + std::to_string(rank);
      copy.checksum_sha256 = std::string(64, 'a');
      ASSERT_TRUE(catalog.commit(copy));
    }
  const auto result = catalog.freshest_surviving(
      "job", {"large", "small"}, {}, kTraceNs + 10,
      [](const ReplicaCheckpoint &copy) {
        return (copy.replica_node_id == "large" ? 9u : 8u) >=
               (copy.rank == 0 ? 9u : 8u);
      });
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result.front().sequence, 1u);
  EXPECT_EQ(result[0].replica_node_id, "large");
  EXPECT_EQ(result[1].replica_node_id, "small");
}
