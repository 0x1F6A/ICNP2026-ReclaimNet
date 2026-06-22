#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/migration_client.h"
#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/time.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <netinet/in.h>
#include <thread>
using namespace reclaimnet;
using namespace reclaimnet::agent;
namespace {
namespace fs = std::filesystem;
std::string read(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}
void write(const fs::path &path, const std::string &data) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(data.data(), data.size());
}
class NodeRuntimeTest : public ::testing::Test {
protected:
  void SetUp() override {
    directory = fs::temp_directory_path() /
                ("reclaimnet-runtime-" + std::to_string(now_ns()));
    fs::create_directories(directory);
    JobRuntimeSpec command_spec;
    auto generated = run_runtime_command(
        {"openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days",
         "1", "-subj", "/CN=localhost", "-addext",
         "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout",
         (directory / "key.pem").string(), "-out",
         (directory / "cert.pem").string()},
        command_spec);
    ASSERT_TRUE(generated);
    tls = {(directory / "cert.pem").string(), (directory / "key.pem").string(),
           (directory / "cert.pem").string(), "localhost", 3.0};
    source = std::make_unique<CheckpointManager>(
        (directory / "source").string(), source_runtime);
    target = std::make_unique<CheckpointManager>(
        (directory / "target").string(), target_runtime);
    JobRuntimeSpec source_spec;
    source_spec.job_id = "training";
    source_spec.state_path = (directory / "source-state.bin").string();
    JobRuntimeSpec target_spec = source_spec;
    target_spec.state_path = (directory / "target-state.bin").string();
    ASSERT_TRUE(source_runtime.register_job(source_spec));
    ASSERT_TRUE(target_runtime.register_job(target_spec));
    write(source_spec.state_path,
          "model.weights=3.125\noptimizer.momentum=0.75\nstep=42\n");
    write(target_spec.state_path, "old-state");
    server = std::make_unique<MigrationServer>(tls, *target);
    ASSERT_TRUE(server->start(0, "127.0.0.1"));
    client = std::make_unique<MigrationClient>(tls);
  }
  void TearDown() override {
    if (server)
      server->stop();
    fs::remove_all(directory);
  }
  MigrationParams params(bool restore) {
    auto captured = source->take_checkpoint("training", 1, {});
    EXPECT_TRUE(captured);
    auto meta = source->latest_meta("training");
    EXPECT_TRUE(meta);
    MigrationParams result;
    result.job_id = "training";
    result.dst_host = "127.0.0.1";
    result.dst_port = server->port();
    result.checkpoint = meta.value();
    result.checkpoint_path = meta.value().path;
    result.allocated_bps = 1000000000.0;
    result.deadline_s = 5;
    result.restore_after_transfer = restore;
    return result;
  }
  fs::path directory;
  RuntimeManager source_runtime, target_runtime;
  MigrationTlsConfig tls;
  std::unique_ptr<CheckpointManager> source, target;
  std::unique_ptr<MigrationServer> server;
  std::unique_ptr<MigrationClient> client;
};
TEST_F(NodeRuntimeTest, UnknownRuntimeDoesNotProduceSyntheticCheckpoint) {
  EXPECT_FALSE(source->take_checkpoint("unknown", 1000, {}));
  EXPECT_FALSE(source->exists("unknown"));
}
TEST_F(NodeRuntimeTest, StoppedRuntimeCannotCapture) {
  ASSERT_TRUE(source->take_checkpoint("training", 1, {}));
  const auto committed = source->latest_meta("training").value();
  ASSERT_TRUE(source_runtime.stop("training"));
  write(directory / "source-state.bin", "state-after-the-source-stopped");
  EXPECT_FALSE(source->take_checkpoint("training", 1, {}));
  EXPECT_FALSE(source->take_checkpoint("training", 1, {}, true));
  EXPECT_EQ(source->latest_meta("training").value().sequence,
            committed.sequence);
  EXPECT_EQ(source->latest_meta("training").value().sha256, committed.sha256);
  EXPECT_EQ(read(committed.path),
            "model.weights=3.125\noptimizer.momentum=0.75\nstep=42\n");
}
TEST_F(NodeRuntimeTest,
       FlowPolicyIsInstalledBeforeTcpConnectAndRemovedOnConnectFailure) {
  auto transfer = params(false);
  const auto destination = server->port();
  server->stop();
  unsigned started = 0, finished = 0;
  FiveTuple configured{};
  transfer.flow_started = [&](const FiveTuple &flow) -> VoidResult {
    ++started;
    configured = flow;
    EXPECT_EQ(flow.src_ip, 0x7f000001U);
    EXPECT_EQ(flow.dst_ip, 0x7f000001U);
    EXPECT_GT(flow.src_port, 0U);
    EXPECT_EQ(flow.dst_port, destination);
    return server->start(destination, "127.0.0.1");
  };
  transfer.flow_finished = [&](const FiveTuple &flow) {
    ++finished;
    EXPECT_EQ(flow.src_ip, configured.src_ip);
    EXPECT_EQ(flow.src_port, configured.src_port);
    EXPECT_EQ(flow.dst_port, configured.dst_port);
  };
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_EQ(started, 1U);
  EXPECT_EQ(finished, 1U);
  server->stop();
  transfer.flow_started = [&](const FiveTuple &flow) -> VoidResult {
    ++started;
    configured = flow;
    return ok_void();
  };
  EXPECT_FALSE(client->send(transfer, {}, cancel));
  EXPECT_EQ(started, 2U);
  EXPECT_EQ(finished, 2U);
}
TEST_F(NodeRuntimeTest, MigrationRequiresTheCommittedManifest) {
  MigrationParams transfer;
  transfer.job_id = "training";
  transfer.dst_host = "127.0.0.1";
  transfer.dst_port = server->port();
  transfer.checkpoint_path = (directory / "source-state.bin").string();
  transfer.allocated_bps = 1000000;
  transfer.deadline_s = 1;
  std::atomic<bool> cancel{false};
  EXPECT_FALSE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(directory / "target-state.bin"), "old-state");
  EXPECT_FALSE(target->exists("training"));
}
TEST_F(NodeRuntimeTest, PurgeRemovesAllSourceCheckpointGenerations) {
  ASSERT_TRUE(source->take_checkpoint("training", 1, {}));
  const auto first = source->latest_meta("training").value().path;
  write(directory / "source-state.bin", "next-committed-state");
  ASSERT_TRUE(source->take_checkpoint("training", 1, {}));
  const auto second = source->latest_meta("training").value().path;
  EXPECT_TRUE(fs::exists(first));
  EXPECT_TRUE(fs::exists(second));
  ASSERT_TRUE(source->purge("training"));
  EXPECT_FALSE(fs::exists(first));
  EXPECT_FALSE(fs::exists(second));
  EXPECT_FALSE(source->exists("training"));
}
TEST_F(NodeRuntimeTest,
       DuplicateStopRetriesCleanupWithoutExecutingApplicationStopAgain) {
  unsigned attempts = 0;
  RuntimeManager runtime([&](const JobRuntimeSpec &) -> VoidResult {
    if (++attempts == 1)
      return Error{"GPU cleanup has not finished"};
    return ok_void();
  });
  auto spec = source_runtime.spec("training").value();
  const auto counter = directory / "stops";
  spec.stop_command = {
      "python3", "-c",
      "import pathlib,sys; path=pathlib.Path(sys.argv[1]); "
      "path.write_text(str(int(path.read_text())+1 if path.exists() else 1))",
      counter.string()};
  spec.start_command = {"true"};
  ASSERT_TRUE(runtime.register_job(spec));
  EXPECT_FALSE(runtime.stop("training"));
  ASSERT_TRUE(runtime.stop("training"));
  EXPECT_EQ(read(counter), "1");
  ASSERT_TRUE(runtime.start("training"));
  ASSERT_TRUE(runtime.stop("training"));
  EXPECT_EQ(read(counter), "2");
}
TEST_F(NodeRuntimeTest, FinalCaptureQuiescesBeforeSavingAndResumesOnFailure) {
  const auto marker = directory / "paused";
  auto spec = source_runtime.spec("training").value();
  spec.backend = "hook";
  spec.pause_command = {"touch", marker.string()};
  spec.resume_command = {"rm", "-f", marker.string()};
  spec.checkpoint_command = {
      "python3",
      "-c",
      "import pathlib,shutil,sys; assert pathlib.Path(sys.argv[3]).exists(); "
      "shutil.copyfile(sys.argv[1],sys.argv[2])",
      "{state_path}",
      "{checkpoint_path}",
      marker.string()};
  spec.restore_command = {"cp", "{checkpoint_path}", "{state_path}"};
  ASSERT_TRUE(source_runtime.register_job(spec));
  ASSERT_TRUE(source->take_checkpoint("training", 1, {}, true));
  EXPECT_TRUE(fs::exists(marker));
  ASSERT_TRUE(source_runtime.resume("training"));
  spec.checkpoint_command = {"false"};
  ASSERT_TRUE(source_runtime.register_job(spec));
  EXPECT_FALSE(source->take_checkpoint("training", 1, {}, true));
  EXPECT_FALSE(fs::exists(marker));
}
TEST_F(NodeRuntimeTest,
       CaptureAndRestoreDeadlinesTerminateHookProcessGroupsAndPreserveSource) {
  auto transfer = params(true);
  const auto pause_marker = directory / "paused";
  const auto late_child = directory / "late-child";
  const std::string child = "import pathlib,sys,time; time.sleep(0.4); "
                            "pathlib.Path(sys.argv[1]).write_text('late')";
  const std::vector<std::string> slow{
      "python3", "-c",
      "import subprocess,sys,time; "
      "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]); "
      "time.sleep(5)",
      child, late_child.string()};
  auto source_spec = source_runtime.spec("training").value();
  source_spec.backend = "hook";
  source_spec.checkpoint_command = slow;
  source_spec.restore_command = {"cp", "{checkpoint_path}", "{state_path}"};
  source_spec.pause_command = {"touch", pause_marker.string()};
  source_spec.resume_command = {"rm", "-f", pause_marker.string()};
  ASSERT_TRUE(source_runtime.register_job(source_spec));
  auto started = std::chrono::steady_clock::now();
  EXPECT_FALSE(
      source->take_checkpoint("training", 1, {}, true, now_ns() + 100000000));
  EXPECT_LT(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - started)
          .count(),
      1.0);
  EXPECT_FALSE(fs::exists(pause_marker));
  EXPECT_TRUE(source->exists("training"));
  auto target_spec = target_runtime.spec("training").value();
  target_spec.restore_command = slow;
  ASSERT_TRUE(target_runtime.register_job(target_spec));
  transfer.deadline_s = 0.1;
  std::atomic<bool> cancel{false};
  EXPECT_FALSE(client->send(transfer, {}, cancel));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_FALSE(fs::exists(late_child));
  EXPECT_EQ(read(directory / "target-state.bin"), "old-state");
  EXPECT_TRUE(source->exists("training"));
  ASSERT_TRUE(target->latest_meta("training"));
  EXPECT_FALSE(target->latest_meta("training").value().restored);
}
TEST_F(NodeRuntimeTest,
       CheckpointContainsActualApplicationStateAndDetectsCorruption) {
  auto transfer = params(false);
  EXPECT_EQ(read(transfer.checkpoint.path),
            read(directory / "source-state.bin"));
  EXPECT_EQ(transfer.checkpoint.size_bytes,
            read(directory / "source-state.bin").size());
  EXPECT_EQ(transfer.checkpoint.sha256,
            checkpoint_sha256(transfer.checkpoint.path).value());
  write(transfer.checkpoint.path, "corrupt");
  EXPECT_FALSE(source->restore_checkpoint("training", {}));
}
TEST_F(NodeRuntimeTest,
       LateCheckpointCannotRollBackTheCommittedApplicationState) {
  auto older = params(false);
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(older, {}, cancel));
  write(directory / "source-state.bin", "newer-model-and-optimizer");
  auto newer = params(true);
  ASSERT_TRUE(client->send(newer, {}, cancel));
  const auto committed = target->latest_meta("training").value();
  EXPECT_FALSE(client->send(older, {}, cancel));
  EXPECT_EQ(target->latest_meta("training").value().sequence,
            committed.sequence);
  EXPECT_EQ(read(directory / "target-state.bin"), "newer-model-and-optimizer");
  auto conflicting = newer.checkpoint;
  conflicting.sha256 = older.checkpoint.sha256;
  conflicting.size_bytes = older.checkpoint.size_bytes;
  EXPECT_FALSE(target->commit_received(conflicting, older.checkpoint.path));
  EXPECT_EQ(target->latest_meta("training").value().sha256, committed.sha256);
}
TEST_F(NodeRuntimeTest,
       PreparedIdentityIsCheckedBeforeAnyDestinationStateChanges) {
  auto transfer = params(true);
  transfer.transfer_id = "unprepared";
  server->stop();
  server = std::make_unique<MigrationServer>(
      tls, *target, MigrationReceiveProgressCb{}, MigrationCommittedCb{},
      [](const CheckpointMeta &meta, bool restoring,
         const std::string &peer) -> VoidResult {
        if (meta.transfer_id != "prepared" || !restoring || peer != "localhost")
          return Error{
              "Migration does not match the authenticated preparation"};
        return ok_void();
      });
  ASSERT_TRUE(server->start(0, "127.0.0.1"));
  transfer.dst_port = server->port();
  std::atomic<bool> cancel{false};
  EXPECT_FALSE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(directory / "target-state.bin"), "old-state");
  EXPECT_FALSE(target->exists("training"));
  EXPECT_FALSE(fs::exists(
      target->incoming_path("training", transfer.checkpoint.sequence)));
  transfer.transfer_id = "prepared";
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(directory / "target-state.bin"),
            read(directory / "source-state.bin"));
}
TEST_F(NodeRuntimeTest,
       RestartRestoresCommittedStateEvenWhenTheOldProcessAcknowledgedRestore) {
  auto transfer = params(true);
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  write(directory / "target-state.bin", "uncommitted-state-after-restore");
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(directory / "target-state.bin"),
            "uncommitted-state-after-restore");
  server->stop();
  RuntimeManager restarted_runtime;
  ASSERT_TRUE(
      restarted_runtime.register_job(target_runtime.spec("training").value()));
  CheckpointManager restarted(target->dir(), restarted_runtime);
  ASSERT_TRUE(restarted.restore_checkpoint("training", {}));
  EXPECT_EQ(read(directory / "target-state.bin"),
            read(directory / "source-state.bin"));
}
TEST_F(NodeRuntimeTest, PybindBridgeRestoresLiveModelAndOptimizerObjects) {
  const auto application = "runtime_app_" + std::to_string(now_ns());
  const auto marker = directory / "application-mode";
  std::string script = R"PY(import copy
import pathlib
import torch
import reclaimnet_checkpoint_bridge as bridge
torch.manual_seed(19)
model = torch.nn.Linear(4, 2)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
sum(p.square().sum() for p in model.parameters()).backward()
optimizer.step()
runtime = bridge.ApplicationRuntime(model, optimizer, {"step": 9})
expected_model = copy.deepcopy(model.state_dict())
expected_optimizer = copy.deepcopy(optimizer.state_dict())
)PY";
  script += "marker = pathlib.Path(\"" + marker.string() + "\")\n";
  script += R"PY(last_mode = ""
def get_runtime(job_id):
    global last_mode
    mode = marker.read_text() if marker.exists() else ""
    if mode == "mutate" and last_mode != mode:
        with torch.no_grad():
            for parameter in model.parameters():
                parameter.zero_()
        optimizer.state.clear()
        runtime.training_state["step"] = 99
    if mode == "verify":
        for name, actual in model.state_dict().items():
            torch.testing.assert_close(actual, expected_model[name], rtol=0, atol=0)
        actual = optimizer.state_dict()
        assert actual["param_groups"] == expected_optimizer["param_groups"]
        for index, state in expected_optimizer["state"].items():
            for name, value in state.items():
                torch.testing.assert_close(actual["state"][index][name], value, rtol=0, atol=0)
        assert runtime.training_state == {"step": 9}
    last_mode = mode
    return runtime
)PY";
  write(directory / (application + ".py"), script);
  JobRuntimeSpec spec;
  spec.job_id = "embedded";
  spec.backend = "pytorch";
  spec.state_path = (directory / "application-state").string();
  spec.application_module = application;
  spec.bridge_directory = directory.string();
  ASSERT_TRUE(source_runtime.register_job(spec));
  auto captured = source->take_checkpoint("embedded", 1, {});
  if (!captured && captured.error().message ==
                       "Pybind11 application checkpoint support is not built")
    GTEST_SKIP() << captured.error().message;
  ASSERT_TRUE(captured) << (captured ? "" : captured.error().message);
  EXPECT_GT(source->latest_meta("embedded").value().size_bytes, 0U);
  write(marker, "mutate");
  auto restored = source->restore_checkpoint("embedded", {});
  ASSERT_TRUE(restored) << (restored ? "" : restored.error().message);
  write(marker, "verify");
  auto verified = source->take_checkpoint("embedded", 1, {});
  ASSERT_TRUE(verified) << (verified ? "" : verified.error().message);
}
TEST_F(NodeRuntimeTest,
       PybindLifecycleQuiescesAndStopsTheActualTrainingThread) {
  const auto application = "runtime_lifecycle_" + std::to_string(now_ns());
  const auto counter = directory / "training-step";
  std::string script = R"PY(import pathlib
import time
import torch
import reclaimnet_checkpoint_bridge as bridge
model = torch.nn.Linear(2, 1)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
)PY";
  script += "counter = pathlib.Path(\"" + counter.string() + "\")\n";
  script += R"PY(def train(runtime):
    runtime.optimizer.zero_grad()
    runtime.model(torch.ones(1, 2)).square().sum().backward()
    runtime.optimizer.step()
    runtime.training_state["step"] += 1
    temporary = counter.with_suffix(".tmp")
    temporary.write_text(str(runtime.training_state["step"]))
    temporary.replace(counter)
    time.sleep(0.002)
runtime = bridge.ApplicationRuntime(model, optimizer, {"step": 0}, training_step=train, autostart=False)
def get_runtime(job_id):
    return runtime
)PY";
  write(directory / (application + ".py"), script);
  JobRuntimeSpec spec;
  spec.job_id = "lifecycle";
  spec.backend = "pytorch";
  spec.state_path = (directory / "state-lifecycle").string();
  spec.application_module = application;
  spec.bridge_directory = directory.string();
  ASSERT_TRUE(source_runtime.register_job(spec));
  auto started = source_runtime.start("lifecycle");
  if (!started && started.error().message ==
                      "Pybind11 application checkpoint support is not built")
    GTEST_SKIP() << started.error().message;
  ASSERT_TRUE(started) << (started ? "" : started.error().message);
  auto steps = [&] {
    return fs::exists(counter) ? std::stoul(read(counter)) : 0UL;
  };
  auto wait = [&](unsigned long minimum) {
    for (int i = 0; i < 200 && steps() < minimum; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    return steps() >= minimum;
  };
  ASSERT_TRUE(wait(3));
  ASSERT_TRUE(source_runtime.pause("lifecycle"));
  const auto paused = steps();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(steps(), paused);
  ASSERT_TRUE(source_runtime.resume("lifecycle"));
  ASSERT_TRUE(wait(paused + 3));
  ASSERT_TRUE(source_runtime.stop("lifecycle"));
  const auto stopped = steps();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(steps(), stopped);
}
TEST_F(NodeRuntimeTest,
       PeriodicReplicaIsDurableWithoutStartingDestinationWork) {
  auto transfer = params(false);
  std::atomic<bool> cancel{false};
  auto sent = client->send(transfer, {}, cancel);
  ASSERT_TRUE(sent) << (sent ? "" : sent.error().message);
  EXPECT_EQ(read(directory / "target-state.bin"), "old-state");
  auto replica = target->latest_meta("training");
  ASSERT_TRUE(replica);
  EXPECT_EQ(replica.value().sha256, transfer.checkpoint.sha256);
  EXPECT_FALSE(replica.value().restored);
  transfer.restore_after_transfer = true;
  sent = client->send(transfer, {}, cancel);
  ASSERT_TRUE(sent) << (sent ? "" : sent.error().message);
  EXPECT_EQ(read(directory / "target-state.bin"),
            read(directory / "source-state.bin"));
  EXPECT_TRUE(target->latest_meta("training").value().restored);
  write(directory / "target-state.bin", "advanced-training-state");
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(directory / "target-state.bin"), "advanced-training-state");
}
TEST_F(NodeRuntimeTest, CancelledTransferResumesFromDurablePartialBytes) {
  write(directory / "source-state.bin", std::string(4 * 65536, 'x'));
  auto transfer = params(false);
  std::atomic<bool> cancel{false};
  auto first = client->send(
      transfer,
      [&](uint64_t bytes) {
        if (bytes >= 65536)
          cancel = true;
      },
      cancel);
  EXPECT_FALSE(first);
  EXPECT_TRUE(source->exists("training"));
  auto partial =
      target->incoming_path("training", transfer.checkpoint.sequence);
  for (int attempt = 0;
       attempt < 100 && (!fs::exists(partial) || fs::file_size(partial) == 0);
       ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  cancel = false;
  uint64_t resumed = 0;
  bool initial = true;
  auto second = client->send(
      transfer,
      [&](uint64_t bytes) {
        if (initial) {
          resumed = bytes;
          initial = false;
        }
      },
      cancel);
  ASSERT_TRUE(second) << (second ? "" : second.error().message);
  EXPECT_GE(resumed, 65536U);
  EXPECT_EQ(target->latest_meta("training").value().sha256,
            transfer.checkpoint.sha256);
}
TEST_F(NodeRuntimeTest, DeadlineAndPeerIdentityFailuresKeepSourceCheckpoint) {
  auto transfer = params(true);
  transfer.deadline_s = 0.03;
  transfer.allocated_bps = 1;
  std::atomic<bool> cancel{false};
  EXPECT_FALSE(client->send(transfer, {}, cancel));
  EXPECT_TRUE(source->exists("training"));
  auto wrong_tls = tls;
  wrong_tls.server_name = "other-provider";
  MigrationClient wrong(wrong_tls);
  transfer.deadline_s = 1;
  transfer.allocated_bps = 1000000000;
  EXPECT_FALSE(wrong.send(transfer, {}, cancel));
  EXPECT_TRUE(source->exists("training"));
}
TEST_F(NodeRuntimeTest, DynamicRateAndActualTcpFlowCallbacksAreApplied) {
  auto transfer = params(false);
  double rate = 0;
  bool started = false, finished = false;
  transfer.rate_provider = [&] { return rate; };
  transfer.flow_started = [&](const FiveTuple &flow) {
    started = true;
    EXPECT_EQ(flow.protocol, IPPROTO_TCP);
    EXPECT_EQ(flow.dst_port, server->port());
    EXPECT_NE(flow.src_port, 0);
    rate = 1000000000;
    return ok_void();
  };
  transfer.flow_finished = [&](const FiveTuple &) { finished = true; };
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_TRUE(started);
  EXPECT_TRUE(finished);
}
TEST_F(NodeRuntimeTest, SlowTransferDoesNotBlockAnIndependentJob) {
  auto slow = params(false);
  slow.allocated_bps = 1;
  std::atomic<bool> slow_cancel{false}, connected{false};
  std::thread slow_worker([&] {
    client->send(slow, [&](uint64_t) { connected = true; }, slow_cancel);
  });
  for (int attempt = 0; attempt < 100 && !connected; ++attempt)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto source_spec = source_runtime.spec("training").value();
  source_spec.job_id = "independent";
  auto target_spec = source_spec;
  target_spec.state_path = (directory / "independent.bin").string();
  EXPECT_TRUE(source_runtime.register_job(source_spec));
  EXPECT_TRUE(target_runtime.register_job(target_spec));
  EXPECT_TRUE(source->take_checkpoint("independent", 1, {}));
  auto fast = slow;
  fast.job_id = "independent";
  fast.checkpoint = source->latest_meta("independent").value();
  fast.checkpoint_path = fast.checkpoint.path;
  fast.allocated_bps = 1000000000;
  fast.deadline_s = 1;
  std::atomic<bool> fast_cancel{false};
  auto result = client->send(fast, {}, fast_cancel);
  slow_cancel = true;
  slow_worker.join();
  EXPECT_TRUE(result) << (result ? "" : result.error().message);
  EXPECT_TRUE(target->exists("independent"));
}
TEST_F(NodeRuntimeTest, DestinationRestoreFailureCannotProduceSuccessAck) {
  auto transfer = params(true);
  auto spec = target_runtime.spec("training").value();
  spec.restore_command = {"false"};
  ASSERT_TRUE(target_runtime.register_job(spec));
  std::atomic<bool> cancel{false};
  auto result = client->send(transfer, {}, cancel);
  EXPECT_FALSE(result);
  EXPECT_TRUE(source->exists("training"));
  EXPECT_FALSE(target->latest_meta("training").value().restored);
}
TEST_F(NodeRuntimeTest, GangStateRestoreWaitsForExplicitStartBarrier) {
  auto transfer = params(true);
  auto spec = target_runtime.spec("training").value();
  const auto started = directory / "started";
  spec.defer_start = true;
  spec.start_command = {"touch", started.string()};
  ASSERT_TRUE(target_runtime.register_job(spec));
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  EXPECT_EQ(read(spec.state_path), read(directory / "source-state.bin"));
  EXPECT_FALSE(fs::exists(started));
  ASSERT_TRUE(target_runtime.start("training"));
  EXPECT_TRUE(fs::exists(started));
}
TEST_F(NodeRuntimeTest,
       EmergencyUsesCommittedStateAndZeroGraceRequestsReplicaRecovery) {
  auto transfer = params(false);
  auto saved = source->latest_meta("training").value();
  std::atomic<bool> cancel{false};
  ASSERT_TRUE(client->send(transfer, {}, cancel));
  write(directory / "source-state.bin", "new-uncommitted-work");
  bool recovered = false;
  ReclaimCallbacks callbacks;
  callbacks.recover = [&](const ReclaimNotification &) {
    recovered = true;
    return target->restore_checkpoint("training", {});
  };
  ReclaimHandler handler(
      *source, *client,
      [&](const ReclaimNotification &) -> Result<ReclaimResponse> {
        ADD_FAILURE()
            << "Emergency reclaim must recover the surviving committed replica";
        return Error{
            "Emergency reclaim does not send from the reclaimed source"};
      },
      callbacks);
  ASSERT_TRUE(handler.handle({"training", 0, ReclaimType::EMERGENCY, 1}));
  EXPECT_TRUE(recovered);
  EXPECT_TRUE(source->exists("training"));
  ASSERT_TRUE(handler.handle({"training", 5, ReclaimType::EMERGENCY, 1}));
  EXPECT_EQ(read(directory / "target-state.bin"),
            "model.weights=3.125\noptimizer.momentum=0.75\nstep=42\n");
  EXPECT_TRUE(source->exists("training"));
  EXPECT_EQ(source->latest_meta("training").value().sequence, saved.sequence);
}
}
