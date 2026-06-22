#include "reclaimnet/agent.h"
#include "reclaimnet/coordinator.h"
#include "reclaimnet/grpc_service.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/runtime.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include "reclaimnet/trace.h"
#include <algorithm>
#include <arpa/inet.h>
#include <filesystem>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
using namespace reclaimnet;
using namespace reclaimnet::agent;
using namespace reclaimnet::control;
namespace {
namespace fs = std::filesystem;
class FixtureGpu : public GpuBackend {
public:
  explicit FixtureGpu(std::string name) : name_(std::move(name)) {}
  Result<std::vector<GpuDevice>> inventory() override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<GpuDevice> devices;
    for (unsigned index = 0; index < 2; ++index) {
      uint64_t used = 0;
      for (const auto &allocation : allocations_)
        if (allocation.second.first == index)
          used += allocation.second.second;
      const auto free = reported_free_bytes.load();
      devices.push_back({index, "GPU-" + name_ + "-" + std::to_string(index),
                         "test-device", 8ULL << 30,
                         free == std::numeric_limits<uint64_t>::max()
                             ? (8ULL << 30) - used
                             : free,
                         0, 8.0, mig_enabled.load(), mig_enabled.load()});
    }
    return devices;
  }
  Result<std::string> create_mig(unsigned index, unsigned, unsigned,
                                 uint64_t bytes) override {
    if (!mig_enabled)
      return Error{"Fixture MIG is disabled"};
    std::lock_guard<std::mutex> lock(mutex_);
    auto id = "MIG-" + name_ + "-" + std::to_string(index) + "-" +
              std::to_string(++sequence_);
    allocations_[id] = {index, bytes};
    return id;
  }
  VoidResult destroy_mig(const std::string &id) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!allocations_.erase(id))
      return Error{"Unknown fixture MIG device"};
    ++destroyed;
    return ok_void();
  }
  Result<uint64_t> zero_free_vram(const std::string &) override {
    ++scrubbed;
    return uint64_t(8ULL << 30);
  }
  std::set<std::string> allocations(uint64_t bytes = 0) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<std::string> result;
    for (const auto &allocation : allocations_)
      if (!bytes || allocation.second.second == bytes)
        result.insert(allocation.first);
    return result;
  }
  std::atomic<unsigned> scrubbed{0};
  std::atomic<unsigned> destroyed{0};
  std::atomic<bool> mig_enabled{false};
  std::atomic<uint64_t> reported_free_bytes{
      std::numeric_limits<uint64_t>::max()};

private:
  std::string name_;
  std::mutex mutex_;
  unsigned sequence_ = 0;
  std::map<std::string, std::pair<unsigned, uint64_t>> allocations_;
};
std::string file_text(const fs::path &path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}
void save_text(const fs::path &path, const std::string &text) {
  std::ofstream output(path);
  output << text;
}
class AgentE2E : public ::testing::Test {
protected:
  uint16_t port() {
    while (true) {
      int fd = socket(AF_INET, SOCK_STREAM, 0);
      sockaddr_in address{};
      address.sin_family = AF_INET;
      address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address));
      socklen_t size = sizeof(address);
      getsockname(fd, reinterpret_cast<sockaddr *>(&address), &size);
      close(fd);
      auto p = ntohs(address.sin_port);
      if (ports.insert(p).second)
        return p;
    }
  }
  VoidResult command(std::vector<std::string> args) {
    return run_runtime_command(args, {});
  }
  void lifecycle(proto::RuntimeSpec *spec, const JobId &job,
                 const fs::path &source_state) {
    for (const auto &arg : std::vector<std::string>{
             "python3", "-c",
             "import json,pathlib,sys,time\nstate=pathlib.Path(sys.argv[1]); "
             "stopped=pathlib.Path(sys.argv[2]); "
             "events=pathlib.Path(sys.argv[3]); accepted=False\n"
             "for row in events.read_text().splitlines() if events.exists() "
             "else []:\n"
             " try: event=json.loads(row)\n except ValueError: continue\n"
             " if event.get('job_id')==sys.argv[5] and "
             "event.get('node_id')=='node-source' and "
             "str(event.get('type','')).endswith('SOURCE_STOPPED'): "
             "accepted=True\n"
             "if not stopped.exists(): "
             "pathlib.Path(sys.argv[4]).write_text('started-before-source-stop'"
             ")\n"
             "with pathlib.Path(str(state)+'.started').open('a') as output: "
             "output.write(json.dumps({'timestamp_ns':time.time_ns(),'source_"
             "stopped_ack':accepted})+'\\n')",
             "{state_path}", source_state.string() + ".stopped",
             (directory / "trace/events.jsonl").string(),
             (directory / ("premature-start-" + job)).string(), job})
      spec->add_start_command(arg);
    for (const auto &arg : std::vector<std::string>{
             "python3", "-c",
             "import pathlib,sys,time; time.sleep(0.15); "
             "pathlib.Path(sys.argv[1]).write_text(str(time.time_ns()))",
             "{state_path}.stopped"})
      spec->add_stop_command(arg);
  }
  Result<proto::ReclaimSignalResponse>
  migration_decision(const ReclaimNotification &notification) {
    auto bundle = load_tls_bundle((directory / "node-source.crt").string(),
                                  (directory / "node-source.key").string(),
                                  (directory / "ca.crt").string());
    if (!bundle)
      return bundle.error();
    auto credentials = make_client_credentials(bundle.value());
    if (!credentials)
      return credentials.error();
    auto stamp = now_ns() / 1000000000;
    auto token =
        issue_jwt_rs256({"node-source", "node-source", stamp, stamp + 300},
                        file_text(directory / "coordinator.key"));
    if (!token)
      return token.error();
    grpc::ClientContext context;
    context.AddMetadata("authorization", "Bearer " + token.value());
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::seconds(5));
    proto::ReclaimSignalRequest request;
    request.set_node_id("node-source");
    request.set_job_id(notification.job_id);
    request.set_deadline_s(notification.deadline_s);
    request.set_event_timestamp_ns(notification.event_timestamp_ns);
    request.set_reclaim_type(notification.type == ReclaimType::EMERGENCY
                                 ? proto::RECLAIM_EMERGENCY
                                 : proto::RECLAIM_SCHEDULED);
    proto::ReclaimSignalResponse response;
    auto channel =
        grpc::CreateChannel(coordinator_address, credentials.value());
    auto status = proto::CoordinatorService::NewStub(channel)->ReclaimSignal(
        &context, request, &response);
    if (!status.ok())
      return Error{status.error_message()};
    return response;
  }
  void certificate(const std::string &node) {
    auto key = (directory / (node + ".key")).string(),
         request = (directory / (node + ".csr")).string();
    ASSERT_TRUE(
        command({"openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
                 "-subj", "/CN=" + node, "-keyout", key, "-out", request}));
    save_text(directory / (node + ".ext"),
              "subjectAltName=DNS:" + node +
                  ",DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,"
                  "clientAuth\n");
    ASSERT_TRUE(
        command({"openssl", "x509", "-req", "-in", request, "-CA",
                 (directory / "ca.crt").string(), "-CAkey",
                 (directory / "ca.key").string(), "-CAcreateserial", "-days",
                 "1", "-out", (directory / (node + ".crt")).string(),
                 "-extfile", (directory / (node + ".ext")).string()}));
  }
  void SetUp() override {
#if !RECLAIMNET_HAVE_JSON
    GTEST_SKIP() << "JSON topology support is disabled";
#else
    directory = fs::temp_directory_path() /
                ("reclaimnet-agent-e2e-" + std::to_string(now_ns()));
    fs::create_directories(directory);
    ASSERT_TRUE(command({"openssl", "req", "-x509", "-newkey", "rsa:2048",
                         "-nodes", "-days", "1", "-subj", "/CN=fixture-ca",
                         "-keyout", (directory / "ca.key").string(), "-out",
                         (directory / "ca.crt").string()}));
    certificate("coordinator");
    for (const auto &node : node_ids)
      certificate(node);
    ASSERT_TRUE(command({"openssl", "pkey", "-in",
                         (directory / "coordinator.key").string(), "-pubout",
                         "-out", (directory / "coordinator.pub").string()}));
    std::string topology = "{\"links\":[";
    bool first = true;
    for (size_t i = 0; i < node_ids.size(); ++i)
      for (size_t j = i + 1; j < node_ids.size(); ++j) {
        if (!first)
          topology += ",";
        first = false;
        topology += "{\"src\":\"" + node_ids[i] + "\",\"dst\":\"" +
                    node_ids[j] +
                    "\",\"bandwidth_bps\":1000000000,\"latency_us\":2,"
                    "\"level\":\"IntraRack\"}";
      }
    save_text(directory / "topology.json", topology + "]}");
    CoordinatorConfig cfg;
    cfg.sqlite_path = ":memory:";
    cfg.topology_file = (directory / "topology.json").string();
    cfg.epoch_seconds = 0.02;
    cfg.restore_time_s = 0.05;
    cfg.minimum_useful_transfer_s = 0.01;
    cfg.B_ckpt_bps = 1000000;
    cfg.p2_k_min = 2;
    cfg.jwt_enabled = true;
    cfg.trace_dir = (directory / "trace").string();
    cfg.run_id = "agent-e2e";
    cfg.jwt_private_key_path = (directory / "coordinator.key").string();
    cfg.jwt_public_key_path = (directory / "coordinator.pub").string();
    cfg.tls_cert_path = (directory / "coordinator.crt").string();
    cfg.tls_key_path = (directory / "coordinator.key").string();
    cfg.tls_ca_path = (directory / "ca.crt").string();
    cfg.mtls_enabled = true;
    coordinator = std::make_unique<Coordinator>(cfg);
    auto started = coordinator->start();
    ASSERT_TRUE(started) << (started ? "" : started.error().message);
    service = std::make_unique<CoordinatorServiceImpl>(*coordinator);
    auto bundle =
        load_tls_bundle(cfg.tls_cert_path, cfg.tls_key_path, cfg.tls_ca_path);
    ASSERT_TRUE(bundle);
    auto credentials = make_server_credentials(bundle.value());
    ASSERT_TRUE(credentials);
    grpc::ServerBuilder builder;
    int chosen = 0;
    builder.AddListeningPort("127.0.0.1:0", credentials.value(), &chosen);
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    ASSERT_TRUE(server);
    coordinator_address = "127.0.0.1:" + std::to_string(chosen);
    for (const auto &node : node_ids) {
      AgentConfig config;
      config.node_id = node;
      config.building = "A";
      config.switch_id = "sw";
      config.coordinator_address = coordinator_address;
      config.listen_address = "127.0.0.1:" + std::to_string(port());
      config.advertise_address = config.listen_address;
      config.migration_port = port();
      config.control_poll_s = 0.02;
      config.total_vram_bytes = 8ULL << 30;
      config.link_capacity_bps = 1000000000;
      config.ebpf_enabled = false;
      config.gpu_isolation_enabled = true;
      config.checkpoint_dir = (directory / node).string();
      config.tls_cert_path = (directory / (node + ".crt")).string();
      config.tls_key_path = (directory / (node + ".key")).string();
      config.tls_ca_path = (directory / "ca.crt").string();
      config.jwt_public_key_path = (directory / "coordinator.pub").string();
      auto backend = std::make_shared<FixtureGpu>(node);
      gpus.push_back(backend);
      auto agent = std::make_unique<NodeAgent>(config, backend);
      auto result = agent->start();
      ASSERT_TRUE(result) << (result ? "" : result.error().message);
      agents.push_back(std::move(agent));
    }
    initial =
        "{\"model\":[1.0,2.0],\"optimizer\":{\"momentum\":0.5},\"step\":42}";
    state = directory / "application-state.json";
    save_text(state, initial);
    proto::RuntimeRegistration registration;
    auto *spec = registration.mutable_runtime();
    spec->set_job_id("training");
    spec->set_state_path(state.string());
    spec->set_backend("file");
    lifecycle(spec, "training", state);
    auto *job = registration.mutable_job();
    job->set_job_id("training");
    job->set_node_id("node-source");
    job->set_vram_bytes(1024);
    job->set_checkpoint_size(initial.size());
    job->set_lambda_e(0.01);
    job->set_write_cap_bps(100000);
    registration.set_cuda_capability_required(7.0);
    registration.set_gpu_count_required(1);
    registration.set_rank_count(1);
    registration.add_rank_nodes("node-source");
    registration.set_remaining_runtime_s(60);
    auto registered = agents[0]->register_runtime(registration);
    ASSERT_TRUE(registered) << (registered ? "" : registered.error().message);
    training_registration = registration;
#endif
  }
  void TearDown() override {
    for (auto &agent : agents)
      agent->stop();
    agents.clear();
    if (server)
      server->Shutdown();
    if (coordinator)
      coordinator->stop();
    if (!directory.empty())
      fs::remove_all(directory);
  }
  bool wait_for(const std::function<bool()> &condition,
                int milliseconds = 10000) {
    for (int elapsed = 0; elapsed < milliseconds; elapsed += 20) {
      if (condition())
        return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return condition();
  }
  bool wait_for_replica_and_quiet() {
    if (!wait_for([&] {
          for (const auto &node : node_ids)
            if (node != "node-source" &&
                fs::exists(directory / node / "training.meta"))
              return true;
          return false;
        }))
      return false;
    if (!coordinator->on_hazard_sample("node-source", 0.0, now_ns()))
      return false;
    coordinator->run_one_epoch();
    if (!wait_for([&] {
          auto job = coordinator->jobs().get("training");
          return job && job->checkpoint_interval_s > 10.0;
        }))
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return true;
  }
  void verify_single_start_after_source_stop(const NodeId &owner,
                                             const JobId &job = "training",
                                             const fs::path &source_state = {},
                                             bool require_ack = true) {
#if RECLAIMNET_HAVE_JSON
    const auto source = source_state.empty() ? state : source_state;
    const auto started = directory / owner / ("state-" + job + "-0.started");
    ASSERT_TRUE(wait_for([&] {
      return fs::exists(started) && fs::exists(source.string() + ".stopped");
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    std::ifstream records(started);
    std::string line;
    unsigned count = 0;
    while (std::getline(records, line))
      if (!line.empty()) {
        ++count;
        auto entry = nlohmann::json::parse(line);
        if (require_ack)
          EXPECT_TRUE(entry.at("source_stopped_ack").get<bool>());
        EXPECT_GE(entry.at("timestamp_ns").get<int64_t>(),
                  std::stoll(file_text(source.string() + ".stopped")));
      }
    EXPECT_EQ(count, 1U);
    EXPECT_FALSE(fs::exists(directory / ("premature-start-" + job)));
#endif
  }
  template <typename T> void validate_family(const std::string &name) {
    std::ifstream input(directory / "trace" / name);
    ASSERT_TRUE(input) << name;
    auto parsed = trace::ReadJsonLines<T>(
        input, [](const T &) -> VoidResult { return ok_void(); });
    ASSERT_TRUE(parsed) << (parsed ? "" : name + ": " + parsed.error().message);
    EXPECT_GT(parsed.value(), 0U) << name;
  }
  void validate_trace() {
#if RECLAIMNET_HAVE_JSON
    for (auto &agent : agents)
      agent->stop();
    std::ifstream flows(directory / "trace/flows.jsonl");
    std::string first_flow;
    ASSERT_TRUE(std::getline(flows, first_flow));
    auto measured = trace::Parse<trace::Flow>(first_flow);
    ASSERT_TRUE(measured) << (measured ? "" : measured.error().message);
    ASSERT_TRUE(measured.value().average_rate_bps);
    ASSERT_TRUE(coordinator->on_bandwidth_sample(
        measured.value().src_node_id, measured.value().dst_node_id,
        *measured.value().average_rate_bps, now_ns()));
    coordinator->stop();
    validate_family<trace::BandwidthSample>("bandwidth_samples.jsonl");
    validate_family<trace::HazardSample>("hazard_samples.jsonl");
    validate_family<trace::ReclaimEvent>("reclaims.jsonl");
    validate_family<trace::P1Epoch>("p1_allocation_epochs.jsonl");
    validate_family<trace::Telemetry>("telemetry.jsonl");
    validate_family<trace::Job>("jobs.jsonl");
    validate_family<trace::Flow>("flows.jsonl");
    validate_family<trace::Migration>("migrations.jsonl");
    ASSERT_TRUE(trace::ParseNodes(file_text(directory / "trace/nodes.json")));
    ASSERT_TRUE(trace::Parse<trace::Schema>(
        file_text(directory / "trace/schema.json")));
    auto manifest = trace::Parse<trace::Manifest>(
        file_text(directory / "trace/manifest.json"));
    ASSERT_TRUE(manifest);
    const auto json = nlohmann::json::parse(manifest.value().raw_json);
    EXPECT_DOUBLE_EQ(
        json.at("parameters").at("p1_control_epoch_s").get<double>(), 0.02);
    EXPECT_DOUBLE_EQ(
        json.at("parameters").at("p3_min_useful_transfer_s").get<double>(),
        0.01);
    for (const auto &file : json.at("files").items()) {
      const auto path = directory / "trace" / file.key();
      ASSERT_TRUE(fs::is_regular_file(path));
      EXPECT_EQ(fs::file_size(path), file.value().at("bytes").get<uint64_t>());
      auto hash = sha256_file(path.string());
      ASSERT_TRUE(hash);
      EXPECT_EQ(hash.value(), file.value().at("sha256").get<std::string>());
    }
#endif
  }
  fs::path directory, state;
  std::set<uint16_t> ports;
  std::string coordinator_address, initial;
  std::unique_ptr<Coordinator> coordinator;
  std::unique_ptr<CoordinatorServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
  std::vector<std::unique_ptr<NodeAgent>> agents;
  std::vector<std::shared_ptr<FixtureGpu>> gpus;
  proto::RuntimeRegistration training_registration;
  std::vector<std::string> node_ids{"node-source", "node-b", "node-c",
                                    "node-second", "node-d"};
};
TEST_F(AgentE2E,
       ScheduledReclaimTransfersRestoresAndCommitsOwnerThroughTlsRpc) {
#if RECLAIMNET_HAVE_JSON
  ASSERT_TRUE(wait_for_replica_and_quiet());
  for (const auto &node : node_ids)
    if (node != "node-source")
      EXPECT_FALSE(fs::exists(directory / node / "state-training-0"));
  auto updated = initial;
  updated.replace(updated.find("42"), 2, "43");
  save_text(state, updated);
  auto reclaimed = agents[0]->handle_reclaim(
      {"training", 5.0, ReclaimType::SCHEDULED, 1024});
  ASSERT_TRUE(reclaimed) << (reclaimed ? "" : reclaimed.error().message);
  ASSERT_TRUE(wait_for([&] {
    auto job = coordinator->jobs().get("training");
    return job && job->state == JobState::RUNNING &&
           job->node_id != "node-source";
  }));
  auto owner = coordinator->jobs().get("training")->node_id;
  EXPECT_EQ(file_text(directory / owner / "state-training-0"), updated);
  ASSERT_TRUE(wait_for(
      [&] { return !fs::exists(directory / "node-source/training.meta"); }));
  EXPECT_GT(gpus[0]->scrubbed.load(), 0U);
  verify_single_start_after_source_stop(owner);
  validate_trace();
#endif
}
TEST_F(
    AgentE2E,
    EmergencyZeroGraceRecoversCommittedReplicaWithoutCapturingNewSourceState) {
#if RECLAIMNET_HAVE_JSON
  ASSERT_TRUE(wait_for_replica_and_quiet());
  auto updated = initial;
  updated.replace(updated.find("42"), 2, "43");
  save_text(state, updated);
  auto reclaimed = agents[0]->handle_reclaim(
      {"training", 0.0, ReclaimType::EMERGENCY, 1024});
  ASSERT_TRUE(reclaimed) << (reclaimed ? "" : reclaimed.error().message);
  ASSERT_TRUE(wait_for([&] {
    auto job = coordinator->jobs().get("training");
    return job && job->state == JobState::RUNNING &&
           job->node_id != "node-source";
  }));
  auto owner = coordinator->jobs().get("training")->node_id;
  EXPECT_EQ(file_text(directory / owner / "state-training-0"), initial);
  EXPECT_EQ(file_text(state), updated);
  EXPECT_TRUE(fs::exists(directory / "node-source/training.meta"));
  verify_single_start_after_source_stop(owner, "training", state, false);
  validate_trace();
#endif
}
TEST_F(AgentE2E, DuplicateExecuteMigrationKeepsTheCurrentCaptureAndTransfer) {
#if RECLAIMNET_HAVE_JSON
  ASSERT_TRUE(wait_for_replica_and_quiet());
  const auto captures = directory / "final-captures";
  const auto capturing = directory / "final-capture-started";
  auto registration = training_registration;
  auto *spec = registration.mutable_runtime();
  spec->set_backend("hook");
  for (const auto &arg : std::vector<std::string>{
           "python3", "-c",
           "import pathlib,shutil,sys,time\nsource=pathlib.Path(sys.argv[1]); "
           "target=pathlib.Path(sys.argv[2])\n"
           "if source==pathlib.Path(sys.argv[5]):\n"
           " with pathlib.Path(sys.argv[3]).open('a') as records: "
           "records.write('capture\\n')\n"
           " pathlib.Path(sys.argv[4]).write_text('capturing'); "
           "time.sleep(0.4)\n"
           "shutil.copyfile(source,target)",
           "{state_path}", "{checkpoint_path}", captures.string(),
           capturing.string(), state.string()})
    spec->add_checkpoint_command(arg);
  for (const auto &arg :
       std::vector<std::string>{"cp", "{checkpoint_path}", "{state_path}"})
    spec->add_restore_command(arg);
  ASSERT_TRUE(agents[0]->register_runtime(registration, false, true, false));
  auto updated = initial;
  updated.replace(updated.find("42"), 2, "43");
  save_text(state, updated);
  auto decision =
      migration_decision({"training", 5.0, ReclaimType::SCHEDULED, 1024});
  ASSERT_TRUE(decision) << (decision ? "" : decision.error().message);
  ASSERT_TRUE(decision.value().success());
  ASSERT_EQ(decision.value().ranks_size(), 1);
  proto::MigrationExecution execution;
  execution.set_job_id("training");
  execution.set_migration_id(decision.value().migration_id());
  execution.set_notice_s(5.0);
  *execution.mutable_rank() = decision.value().ranks(0);
  ASSERT_TRUE(agents[0]->execute_migration(execution));
  ASSERT_TRUE(wait_for([&] { return fs::exists(capturing); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_TRUE(agents[0]->execute_migration(execution));
  ASSERT_TRUE(wait_for([&] {
    auto job = coordinator->jobs().get("training");
    return job && job->state == JobState::RUNNING &&
           job->node_id != "node-source";
  }));
  auto owner = coordinator->jobs().get("training")->node_id;
  EXPECT_EQ(file_text(directory / owner / "state-training-0"), updated);
  verify_single_start_after_source_stop(owner);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(file_text(captures), "capture\n");
  EXPECT_FALSE(fs::exists(directory / "node-source/training.meta"));
  std::ifstream flows(directory / "trace/flows.jsonl");
  std::string row;
  unsigned transfers = 0;
  while (std::getline(flows, row))
    if (!row.empty()) {
      auto flow = nlohmann::json::parse(row);
      if (flow.value("migration_id", nlohmann::json()) ==
              execution.migration_id() &&
          flow.value("src_node_id", "") == "node-source") {
        ++transfers;
        EXPECT_EQ(flow.at("status").get<std::string>(), "COMPLETE");
      }
    }
  EXPECT_EQ(transfers, 1U);
  validate_trace();
#endif
}
TEST_F(AgentE2E,
       NodeLevelEmergencyStopsBothJobsAndRecoversTheTwoGpuAllocation) {
#if RECLAIMNET_HAVE_JSON
  ASSERT_TRUE(wait_for_replica_and_quiet());
  for (auto &gpu : gpus)
    gpu->mig_enabled = true;
  auto registration = training_registration;
  auto *spec = registration.mutable_runtime();
  spec->set_job_id("two-gpu");
  spec->clear_start_command();
  spec->clear_stop_command();
  auto second_state = directory / "two-gpu-state.json";
  const auto committed = "{\"model\":[3.0,4.0],\"optimizer\":0.8,\"step\":42}";
  save_text(second_state, committed);
  spec->set_state_path(second_state.string());
  lifecycle(spec, "two-gpu", second_state);
  registration.mutable_job()->set_job_id("two-gpu");
  registration.mutable_job()->set_vram_bytes(4096);
  registration.mutable_job()->set_checkpoint_size(
      std::string(committed).size());
  registration.set_gpu_count_required(2);
  ASSERT_TRUE(agents[0]->register_runtime(registration));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-source", 0.01, now_ns()));
  coordinator->run_one_epoch();
  ASSERT_TRUE(wait_for([&] {
    for (const auto &node : node_ids)
      if (node != "node-source" &&
          fs::exists(directory / node / "two-gpu.meta"))
        return true;
    return false;
  }));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-source", 0.0, now_ns()));
  coordinator->run_one_epoch();
  ASSERT_TRUE(wait_for([&] {
    auto job = coordinator->jobs().get("two-gpu");
    return job && job->checkpoint_interval_s > 10.0;
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto updated = initial;
  updated.replace(updated.find("42"), 2, "43");
  save_text(state, updated);
  auto updated_second = std::string(committed);
  updated_second.replace(updated_second.find("42"), 2, "43");
  save_text(second_state, updated_second);
  auto reclaimed =
      agents[0]->handle_reclaim({"", 0.0, ReclaimType::EMERGENCY, 5120});
  ASSERT_TRUE(reclaimed) << (reclaimed ? "" : reclaimed.error().message);
  ASSERT_TRUE(wait_for([&] {
    for (const auto &job : {"training", "two-gpu"}) {
      auto current = coordinator->jobs().get(job);
      if (!current || current->state != JobState::RUNNING ||
          current->node_id == "node-source")
        return false;
    }
    return true;
  }));
  auto first_owner = coordinator->jobs().get("training")->node_id;
  auto second_owner = coordinator->jobs().get("two-gpu")->node_id;
  EXPECT_EQ(file_text(directory / first_owner / "state-training-0"), initial);
  EXPECT_EQ(file_text(directory / second_owner / "state-two-gpu-0"), committed);
  verify_single_start_after_source_stop(first_owner, "training", state, false);
  verify_single_start_after_source_stop(second_owner, "two-gpu", second_state,
                                        false);
  auto found = std::find(node_ids.begin(), node_ids.end(), second_owner);
  ASSERT_NE(found, node_ids.end());
  EXPECT_EQ(
      gpus[std::distance(node_ids.begin(), found)]->allocations(2048).size(),
      2U);
  EXPECT_EQ(file_text(state), updated);
  EXPECT_EQ(file_text(second_state), updated_second);
  EXPECT_TRUE(fs::exists(directory / "node-source/training.meta"));
  EXPECT_TRUE(fs::exists(directory / "node-source/two-gpu.meta"));
  validate_trace();
#endif
}
TEST_F(AgentE2E, DuplicatePrepareKeepsExistingGpuResourcesWhenFreeVramFalls) {
#if RECLAIMNET_HAVE_JSON
  gpus[1]->mig_enabled = true;
  proto::MigrationPreparation preparation;
  preparation.set_source_node_id("node-source");
  preparation.set_migration_id("prepared-two-gpu");
  preparation.set_rank(0);
  preparation.set_gang_size(1);
  preparation.set_restore_after_transfer(true);
  preparation.set_gpu_count_required(2);
  preparation.set_cuda_capability_required(7.0);
  preparation.set_vram_bytes(4096);
  auto *spec = preparation.mutable_runtime();
  spec->set_job_id("prepared");
  spec->set_backend("file");
  spec->set_state_path(state.string());
  ASSERT_TRUE(agents[1]->prepare_migration(preparation));
  const auto allocated = gpus[1]->allocations();
  ASSERT_EQ(allocated.size(), 2U);
  gpus[1]->reported_free_bytes = 0;
  const auto destroyed = gpus[1]->destroyed.load();
  auto repeated = agents[1]->prepare_migration(preparation);
  ASSERT_TRUE(repeated) << (repeated ? "" : repeated.error().message);
  EXPECT_EQ(gpus[1]->allocations(), allocated);
  EXPECT_EQ(gpus[1]->destroyed.load(), destroyed);
  gpus[1]->reported_free_bytes = std::numeric_limits<uint64_t>::max();
#endif
}
TEST_F(AgentE2E,
       EmergencyWithoutSurvivingReplicaStillStopsTheSourceApplication) {
#if RECLAIMNET_HAVE_JSON
  auto registration = training_registration;
  auto *spec = registration.mutable_runtime();
  spec->set_job_id("unreplicated");
  spec->clear_start_command();
  spec->clear_stop_command();
  auto unreplicated = directory / "unreplicated-state.json";
  save_text(unreplicated, initial);
  spec->set_state_path(unreplicated.string());
  lifecycle(spec, "unreplicated", unreplicated);
  registration.mutable_job()->set_job_id("unreplicated");
  registration.set_loss_budget_s(1e-9);
  ASSERT_TRUE(agents[0]->register_runtime(registration));
  auto reclaimed = agents[0]->handle_reclaim(
      {"unreplicated", 0.0, ReclaimType::EMERGENCY, 1024});
  EXPECT_FALSE(reclaimed);
  EXPECT_TRUE(fs::exists(unreplicated.string() + ".stopped"));
  EXPECT_TRUE(fs::exists(directory / "node-source/unreplicated.meta"));
  for (const auto &node : node_ids)
    if (node != "node-source")
      EXPECT_FALSE(fs::exists(directory / node / "unreplicated.meta"));
  auto job = coordinator->jobs().get("unreplicated");
  ASSERT_TRUE(job);
  EXPECT_EQ(job->state, JobState::SUSPENDED);
#endif
}
TEST_F(AgentE2E, GangEmergencyRestoresAllRanksBeforeStartingTheApplications) {
#if RECLAIMNET_HAVE_JSON
  ASSERT_TRUE(wait_for_replica_and_quiet());
  std::vector<std::string> committed{
      "{\"rank\":0,\"model\":1.25,\"optimizer\":0.5,\"step\":42}",
      "{\"rank\":1,\"model\":2.25,\"optimizer\":0.7,\"step\":42}"};
  proto::RuntimeRegistration registration;
  auto *spec = registration.mutable_runtime();
  spec->set_job_id("gang");
  spec->set_backend("hook");
  spec->add_checkpoint_command("cp");
  spec->add_checkpoint_command("{state_path}");
  spec->add_checkpoint_command("{checkpoint_path}");
  for (const auto &arg : std::vector<std::string>{
           "python3", "-c",
           "import pathlib,shutil,sys,time; source=pathlib.Path(sys.argv[1]); "
           "target=pathlib.Path(sys.argv[2]); "
           "time.sleep(0.25 if '\"rank\":1' in source.read_text() else 0); "
           "target.parent.mkdir(parents=True,exist_ok=True); "
           "shutil.copyfile(source,target)",
           "{checkpoint_path}", "{state_path}"})
    spec->add_restore_command(arg);
  for (const auto &arg : std::vector<std::string>{
           "python3", "-c",
           "import pathlib,sys; state=pathlib.Path(sys.argv[1]); "
           "restored=list(state.parent.parent.glob('node-*/state-gang-*')); "
           "ready=len([p for p in restored if p.is_file() and not "
           "p.name.endswith('.started') and not "
           "p.name.endswith('.stopped')])==2; "
           "(state.parent.parent/"
           "'premature-gang-start').write_text('premature') if not ready else "
           "None; "
           "pathlib.Path(str(state)+'.started').write_text(state.read_text())",
           "{state_path}"})
    spec->add_start_command(arg);
  spec->add_stop_command("touch");
  spec->add_stop_command("{state_path}.stopped");
  auto *job = registration.mutable_job();
  job->set_job_id("gang");
  job->set_vram_bytes(2048);
  job->set_checkpoint_size(committed[0].size() + committed[1].size());
  job->set_lambda_e(0.02);
  job->set_write_cap_bps(100000);
  registration.set_rank_count(2);
  registration.add_rank_nodes("node-source");
  registration.add_rank_nodes("node-second");
  registration.set_gpu_count_required(1);
  registration.set_cuda_capability_required(7.0);
  registration.set_remaining_runtime_s(60);
  auto second = registration;
  second.set_rank(1);
  second.mutable_job()->set_node_id("node-second");
  auto second_state = directory / "gang-source-rank1.json";
  save_text(second_state, committed[1]);
  second.mutable_runtime()->set_state_path(second_state.string());
  ASSERT_TRUE(agents[3]->register_runtime(second, false));
  auto first_state = directory / "gang-source-rank0.json";
  save_text(first_state, committed[0]);
  registration.mutable_runtime()->set_state_path(first_state.string());
  registration.mutable_job()->set_node_id("node-source");
  ASSERT_TRUE(agents[0]->register_runtime(registration));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-source", 0.01, now_ns()));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-second", 0.01, now_ns()));
  coordinator->run_one_epoch();
  ASSERT_TRUE(wait_for([&] {
    unsigned replicas = 0;
    for (const auto &node : node_ids)
      if (node != "node-source" && node != "node-second" &&
          fs::exists(directory / node / "gang.meta"))
        ++replicas;
    return replicas == 2;
  }));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-source", 0.0, now_ns()));
  ASSERT_TRUE(coordinator->on_hazard_sample("node-second", 0.0, now_ns()));
  coordinator->run_one_epoch();
  ASSERT_TRUE(wait_for([&] {
    auto current = coordinator->jobs().get("gang");
    return current && current->checkpoint_interval_s > 10.0;
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto advanced0 = committed[0], advanced1 = committed[1];
  advanced0.replace(advanced0.find("42"), 2, "43");
  advanced1.replace(advanced1.find("42"), 2, "43");
  save_text(first_state, advanced0);
  save_text(second_state, advanced1);
  auto reclaimed =
      agents[0]->handle_reclaim({"gang", 0.0, ReclaimType::EMERGENCY, 2048});
  ASSERT_TRUE(reclaimed) << (reclaimed ? "" : reclaimed.error().message);
  ASSERT_TRUE(wait_for([&] {
    auto current = coordinator->jobs().get("gang");
    return current && current->state == JobState::RUNNING &&
           current->rank_nodes.size() == 2 &&
           current->rank_nodes[0] != "node-source" &&
           current->rank_nodes[1] != "node-second";
  }));
  auto current = coordinator->jobs().get("gang").value();
  ASSERT_NE(current.rank_nodes[0], current.rank_nodes[1]);
  for (uint32_t rank = 0; rank < 2; ++rank) {
    auto restored = directory / current.rank_nodes[rank] /
                    ("state-gang-" + std::to_string(rank));
    ASSERT_TRUE(
        wait_for([&] { return fs::exists(restored.string() + ".started"); }));
    EXPECT_EQ(file_text(restored), committed[rank]);
    EXPECT_EQ(file_text(restored.string() + ".started"), committed[rank]);
  }
  EXPECT_FALSE(fs::exists(directory / "premature-gang-start"));
  EXPECT_EQ(file_text(first_state), advanced0);
  EXPECT_EQ(file_text(second_state), advanced1);
  ASSERT_TRUE(wait_for([&] {
    return fs::exists(first_state.string() + ".stopped") &&
           fs::exists(second_state.string() + ".stopped");
  }));
  EXPECT_TRUE(fs::exists(directory / "node-source/gang.meta"));
  EXPECT_TRUE(fs::exists(directory / "node-second/gang.meta"));
  validate_trace();
#endif
}
}
