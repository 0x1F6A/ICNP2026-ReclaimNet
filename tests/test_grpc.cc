#include <gtest/gtest.h>

#include "reclaimnet/coordinator.h"
#include "reclaimnet/grpc_client.h"
#include "reclaimnet/grpc_service.h"
#include "reclaimnet/mtls.h"
#include "reclaimnet/runtime.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

namespace {
namespace proto = reclaimnet::proto;
using reclaimnet::agent::CoordinatorClient;
using reclaimnet::agent::LocalTelemetry;
using reclaimnet::control::Coordinator;
using reclaimnet::control::CoordinatorServiceImpl;

constexpr uint64_t kVram = 16ULL * 1024 * 1024 * 1024;
constexpr uint64_t kCheckpoint = 1024 * 1024;

void set_deadline(grpc::ClientContext &context) {
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(3));
}

class GrpcTest : public ::testing::Test {
protected:
  void SetUp() override {
    reclaimnet::CoordinatorConfig cfg;
    cfg.sqlite_path = ":memory:";
    cfg.epoch_seconds = 0.02;
    cfg.heartbeat_timeout_s = 30.0;
    cfg.topology_file.clear();
#if RECLAIMNET_HAVE_JSON
    topology_path =
        std::filesystem::temp_directory_path() /
        ("reclaimnet-grpc-" + std::to_string(reclaimnet::now_ns()) + ".json");
    std::ofstream topology(topology_path);
    topology
        << R"({"links":[{"src":"node-src","dst":"node-dst",)"
           R"("bandwidth_bps":100000000000,"latency_us":2,"level":"IntraRack"}]})";
    topology.close();
    cfg.topology_file = topology_path.string();
#endif
    coordinator = std::make_unique<Coordinator>(cfg);
    auto started = coordinator->start(false);
    ASSERT_TRUE(started) << (started ? "" : started.error().message);
    service = std::make_unique<CoordinatorServiceImpl>(*coordinator);
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    ASSERT_TRUE(server);
    ASSERT_GT(port, 0);
    address = "127.0.0.1:" + std::to_string(port);
    channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub = proto::CoordinatorService::NewStub(channel);
    client = std::make_unique<CoordinatorClient>(address, false, "", "", "");
    ASSERT_TRUE(client->register_node("node-src", "127.0.0.1:50052", "A", "sw",
                                      kVram, 100000000000ULL));
    proto::RegisterRequest request;
    request.set_node_id("node-dst");
    request.set_address("127.0.0.1:50053");
    request.set_total_vram(kVram);
    request.set_link_capacity(100000000000ULL);
    proto::RegisterResponse response;
    grpc::ClientContext context;
    set_deadline(context);
    ASSERT_TRUE(stub->Register(&context, request, &response).ok());
    ASSERT_TRUE(response.accepted());
    telemetry.vram_free_bytes = kVram;
    telemetry.gpu_count = 1;
    telemetry.gpu_free_count = 1;
    ASSERT_TRUE(client->heartbeat("node-src", telemetry));
    ASSERT_TRUE(client->heartbeat("node-dst", telemetry));
  }

  void TearDown() override {
    if (server)
      server->Shutdown();
    if (coordinator)
      coordinator->stop();
    if (!topology_path.empty())
      std::filesystem::remove(topology_path);
  }

  void register_job(const std::string &id) {
    auto result = client->register_job(id, "node-src", kCheckpoint, kCheckpoint,
                                       "user", "test");
    ASSERT_TRUE(result) << (result ? "" : result.error().message);
  }

  std::filesystem::path topology_path;
  std::string address;
  std::unique_ptr<Coordinator> coordinator;
  std::unique_ptr<CoordinatorServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<proto::CoordinatorService::Stub> stub;
  std::unique_ptr<CoordinatorClient> client;
  LocalTelemetry telemetry{};
};

TEST_F(GrpcTest, RegistrationHeartbeatAndTopologyUseTheSameSchema) {
  proto::GetTopologyRequest request;
  proto::GetTopologyResponse response;
  grpc::ClientContext context;
  set_deadline(context);
  ASSERT_TRUE(stub->GetTopology(&context, request, &response).ok());
  EXPECT_EQ(response.node_count(), 2U);
  EXPECT_EQ(coordinator->nodes().get("node-dst")->state,
            reclaimnet::NodeState::ONLINE);
  EXPECT_EQ(coordinator->nodes().get("node-src")->telemetry.vram_free_bytes,
            kVram);
}

TEST_F(GrpcTest, DirectedCapacitySamplesPreserveMeasuredNicTelemetry) {
  telemetry.link_tx_bps = 12345.0;
  telemetry.link_rx_bps = 54321.0;
  telemetry.sampled_ns = reclaimnet::now_ns();
  ASSERT_TRUE(client->heartbeat("node-src", telemetry));
  const auto before = coordinator->nodes().get("node-src").value();
  proto::TelemetryReport report;
  report.set_node_id("node-src");
  report.set_timestamp(telemetry.sampled_ns + 1);
  for (bool reverse : {false, true}) {
    auto *sample = report.add_bw_samples();
    sample->set_node_id("node-src");
    sample->set_src_node_id(reverse ? "node-dst" : "node-src");
    sample->set_dst_node_id(reverse ? "node-src" : "node-dst");
    sample->set_bps(reverse ? 80000000000ULL : 90000000000ULL);
    sample->set_timestamp(report.timestamp());
  }
  proto::Empty response;
  grpc::ClientContext context;
  set_deadline(context);
  ASSERT_TRUE(stub->ReportTelemetry(&context, report, &response).ok());
  const auto after = coordinator->nodes().get("node-src").value();
  EXPECT_DOUBLE_EQ(after.telemetry.link_tx_bps, 12345.0);
  EXPECT_DOUBLE_EQ(after.telemetry.link_rx_bps, 54321.0);
  EXPECT_EQ(after.telemetry.updated_ns, before.telemetry.updated_ns);
  EXPECT_EQ(after.last_heartbeat_ns, before.last_heartbeat_ns);
}

TEST_F(GrpcTest, HazardTelemetryDrivesPlansAndReportsMetrics) {
  ASSERT_TRUE(client->register_job("job-plans", "node-src", kCheckpoint,
                                   kCheckpoint, "user", "test", 8000000.0,
                                   120.0, 0.005));
  proto::TelemetryReport report;
  report.set_node_id("node-src");
  auto *hazard = report.add_hazard_samples();
  hazard->set_node_id("node-src");
  hazard->set_lambda_e(0.002);
  hazard->set_timestamp(reclaimnet::now_ns());
  auto *bandwidth = report.add_bw_samples();
  bandwidth->set_bps(8000000);
  auto *checkpoint = report.add_checkpoints();
  checkpoint->set_job_id("job-plans");
  checkpoint->set_size_bytes(kCheckpoint);
  checkpoint->set_duration_ns(1000000000);
  proto::Empty result;
  grpc::ClientContext context;
  set_deadline(context);
  ASSERT_TRUE(stub->ReportTelemetry(&context, report, &result).ok());
  EXPECT_GT(coordinator->hazard().lambda("node-src"), 0.0);
  EXPECT_EQ(coordinator->nodes().get("node-src")->telemetry.link_tx_bps,
            8000000.0);

  coordinator->run_one_epoch();
  auto plans = client->checkpoint_plans();
  for (int attempt = 0;
       attempt < 50 && (plans.empty() || !plans.front().feasible); ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_TRUE(client->heartbeat("node-src", telemetry));
    plans = client->checkpoint_plans();
  }
  ASSERT_EQ(plans.size(), 1U);
  EXPECT_EQ(plans.front().job_id, "job-plans");
  EXPECT_GT(plans.front().interval_ns, 0U);
  EXPECT_EQ(plans.front().allocated_bps, 8000000U);
  EXPECT_EQ(plans.front().write_rate_limit_bps, 1600000U);
  EXPECT_TRUE(plans.front().feasible);
  EXPECT_EQ(coordinator->jobs().get("job-plans")->loss_budget_s, 120.0);
  EXPECT_EQ(coordinator->jobs().get("job-plans")->lambda_i, 0.005);

  proto::PrometheusSnapshot metrics;
  grpc::ClientContext metrics_context;
  set_deadline(metrics_context);
  ASSERT_TRUE(
      stub->GetPrometheusSnapshot(&metrics_context, result, &metrics).ok());
  EXPECT_NE(metrics.metrics_text().find("reclaimnet_checkpoint_bytes"),
            std::string::npos);
}

TEST_F(GrpcTest, RateOverridesAreExplicitlyRejectedAndValidationFails) {
  proto::RateLimit rate;
  rate.set_rate_bps(100000000000ULL);
  proto::AdmissionResult response;
  grpc::ClientContext context;
  set_deadline(context);
  ASSERT_TRUE(stub->UpdateRateBudget(&context, rate, &response).ok());
  EXPECT_FALSE(response.admitted());
  EXPECT_FALSE(response.reason().empty());
  EXPECT_FALSE(
      client->register_job("unknown-job", "missing-node", 1, 1, "", ""));
  EXPECT_FALSE(
      client->register_job("invalid-cap", "node-src", 1, 1, "", "", -1.0));
  EXPECT_FALSE(
      client->register_job("invalid-loss", "node-src", 1, 1, "", "", 0.0, 0.0));
  EXPECT_FALSE(client->heartbeat("missing-node", telemetry));
}

TEST_F(GrpcTest, ClientDoesNotDowngradeAnInvalidTlsConfiguration) {
  CoordinatorClient secure(address, true, "/missing/cert.pem",
                           "/missing/key.pem", "/missing/ca.pem");
  EXPECT_FALSE(secure.connected());
  auto result =
      secure.register_node("untrusted", "127.0.0.1", "", "", kVram, 1000000000);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message.find("PEM"), std::string::npos);
  EXPECT_EQ(coordinator->nodes().size(), 2U);
}

TEST_F(GrpcTest, TraceNanosecondsSurviveHeartbeatReportsAndMigrationRequests) {
  constexpr int64_t trace_ns = 1772302390292933888LL;
  auto source = coordinator->nodes().get("node-src").value();
  source.registered_at_ns = trace_ns - 3600000000000LL;
  source.last_heartbeat_ns = trace_ns - 1;
  source.telemetry.updated_ns = trace_ns - 1;
  ASSERT_TRUE(coordinator->register_node(source));
  telemetry.sampled_ns = trace_ns;
  ASSERT_TRUE(client->heartbeat("node-src", telemetry));
  EXPECT_EQ(coordinator->nodes().get("node-src")->last_heartbeat_ns, trace_ns);
  EXPECT_EQ(coordinator->nodes().get("node-src")->telemetry.updated_ns,
            trace_ns);

  proto::HeartbeatRequest heartbeat;
  heartbeat.set_node_id("node-src");
  heartbeat.set_free_vram(kVram);
  heartbeat.set_timestamp(trace_ns + 1);
  proto::HeartbeatResponse heartbeat_response;
  grpc::ClientContext heartbeat_context;
  set_deadline(heartbeat_context);
  ASSERT_TRUE(
      stub->Heartbeat(&heartbeat_context, heartbeat, &heartbeat_response).ok());
  EXPECT_EQ(coordinator->nodes().get("node-src")->last_heartbeat_ns,
            trace_ns + 1);
  EXPECT_GT(heartbeat_response.server_time_ns(), trace_ns);

  proto::TelemetryReport report;
  report.set_node_id("node-src");
  report.set_timestamp(trace_ns + 2);
  report.add_bw_samples()->set_bps(8000000);
  report.add_hazard_samples()->set_lambda_e(0.002);
  proto::Empty report_response;
  grpc::ClientContext report_context;
  set_deadline(report_context);
  ASSERT_TRUE(
      stub->ReportTelemetry(&report_context, report, &report_response).ok());
  EXPECT_EQ(coordinator->nodes().get("node-src")->last_heartbeat_ns,
            trace_ns + 2);
  EXPECT_EQ(coordinator->hazard().snapshot().at("node-src").last_sample_ns,
            trace_ns + 2);

  report.mutable_bw_samples(0)->set_timestamp(trace_ns + 3);
  report.mutable_hazard_samples(0)->set_timestamp(trace_ns + 3);
  grpc::ClientContext sample_context;
  set_deadline(sample_context);
  ASSERT_TRUE(
      stub->ReportTelemetry(&sample_context, report, &report_response).ok());
  EXPECT_EQ(coordinator->nodes().get("node-src")->last_heartbeat_ns,
            trace_ns + 3);
  EXPECT_EQ(coordinator->hazard().snapshot().at("node-src").last_sample_ns,
            trace_ns + 3);

#if RECLAIMNET_HAVE_JSON
  register_job("trace-migrate");
  proto::MigrationRequest migration;
  migration.set_job_id("trace-migrate");
  migration.set_src_node_id("node-src");
  migration.set_tau_seconds(20.0);
  migration.set_lambda_e_src(0.007);
  migration.set_event_timestamp_ns(trace_ns + 4);
  proto::MigrationDecision decision;
  grpc::ClientContext migration_context;
  set_deadline(migration_context);
  auto status =
      stub->RequestMigration(&migration_context, migration, &decision);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(coordinator->hazard().snapshot().at("node-src").last_sample_ns,
            trace_ns + 4);
  EXPECT_EQ(decision.deadline_ns(), trace_ns + 4 + 20000000000LL);

  report.mutable_hazard_samples(0)->set_timestamp(trace_ns + 5);
  report.mutable_hazard_samples(0)->set_lambda_e(0.008);
  grpc::ClientContext later_context;
  set_deadline(later_context);
  ASSERT_TRUE(
      stub->ReportTelemetry(&later_context, report, &report_response).ok());
  EXPECT_EQ(coordinator->hazard().snapshot().at("node-src").last_sample_ns,
            trace_ns + 5);
  EXPECT_EQ(coordinator->hazard().estimate_at("node-src", trace_ns + 5).value(),
            0.008);
#endif
}

TEST_F(GrpcTest, MigrationAdmissionAndCompletionRoundTrip) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support is disabled";
#else
  register_job("job-migrate");
  reclaimnet::agent::ReclaimNotification notice{
      "job-migrate", 20.0, reclaimnet::ReclaimType::SCHEDULED, kCheckpoint};
  auto decision = client->reclaim_signal(notice, "node-src");
  ASSERT_TRUE(decision) << (decision ? "" : decision.error().message);
  EXPECT_EQ(decision.value().dst_node, "node-dst");
  EXPECT_GT(decision.value().allocated_bps, 0.0);
  EXPECT_GT(decision.value().deadline_s, 0.0);
  EXPECT_FALSE(client->migration_progress("job-migrate", 1, true));
  EXPECT_TRUE(
      client->migration_progress("job-migrate", kCheckpoint / 2, false));
  EXPECT_TRUE(client->migration_progress("job-migrate", kCheckpoint, true));
  EXPECT_EQ(coordinator->jobs().get("job-migrate")->state,
            reclaimnet::JobState::RUNNING);
#endif
}

TEST_F(GrpcTest, ForeignFinalPayloadDoesNotMutateItsAdmittedMigration) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  register_job("protected-final");
  auto command = coordinator->on_reclaim_signal(
      "node-src", "protected-final", 60, reclaimnet::ReclaimType::SCHEDULED);
  ASSERT_TRUE(command);
  proto::FinalCheckpointRequest request;
  request.set_node_id("node-dst");
  request.set_job_id("protected-final");
  request.set_migration_id(command.value().migration_id);
  request.set_size_bytes(1'000'000'000'000ULL);
  request.set_timestamp_ns(reclaimnet::now_ns());
  proto::RateUpdate response;
  grpc::ClientContext context;
  set_deadline(context);
  auto status = stub->PrepareFinalCheckpoint(&context, request, &response);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::PERMISSION_DENIED);
  EXPECT_EQ(coordinator->jobs().get("protected-final")->state,
            reclaimnet::JobState::MIGRATING);
  auto retry = coordinator->on_reclaim_signal(
      "node-src", "protected-final", 60, reclaimnet::ReclaimType::SCHEDULED);
  ASSERT_TRUE(retry);
  EXPECT_EQ(retry.value().migration_id, command.value().migration_id);
  EXPECT_DOUBLE_EQ(retry.value().allocated_bps, command.value().allocated_bps);
#endif
}

TEST(GrpcAuthentication,
     ForeignProgressCannotMutateSourceButCompletedAckCanRetryAfterMove) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support disabled";
#else
  using namespace reclaimnet;
  struct Cleanup {
    std::filesystem::path directory;
    ~Cleanup() { std::filesystem::remove_all(directory); }
  } cleanup{std::filesystem::temp_directory_path() /
            ("reclaimnet-grpc-auth-" + std::to_string(now_ns()))};
  std::filesystem::create_directories(cleanup.directory);
  const auto path = [&](const std::string &name) {
    return (cleanup.directory / name).string();
  };
  const auto command = [](const std::vector<std::string> &args) {
    return agent::run_runtime_command(args, {});
  };
  ASSERT_TRUE(command({"openssl", "req", "-x509", "-newkey", "rsa:2048",
                       "-nodes", "-days", "1", "-subj", "/CN=fixture-ca",
                       "-keyout", path("ca.key"), "-out", path("ca.crt")}));
  for (const auto *name : {"server", "node-src", "node-other"}) {
    ASSERT_TRUE(command({"openssl", "req", "-new", "-newkey", "rsa:2048",
                         "-nodes", "-subj", std::string("/CN=") + name,
                         "-keyout", path(std::string(name) + ".key"), "-out",
                         path(std::string(name) + ".csr")}));
    {
      std::ofstream extension(path(std::string(name) + ".ext"));
      extension << "subjectAltName=DNS:" << name
                << ",DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,"
                   "clientAuth\n";
    }
    ASSERT_TRUE(command(
        {"openssl", "x509", "-req", "-in", path(std::string(name) + ".csr"),
         "-CA", path("ca.crt"), "-CAkey", path("ca.key"), "-CAcreateserial",
         "-days", "1", "-out", path(std::string(name) + ".crt"), "-extfile",
         path(std::string(name) + ".ext")}));
  }
  ASSERT_TRUE(command({"openssl", "pkey", "-in", path("ca.key"), "-pubout",
                       "-out", path("ca.pub")}));
  {
    std::ofstream topology(path("topology.json"));
    topology
        << R"({"links":[{"src":"node-src","dst":"node-dst",)"
           R"("bandwidth_bps":1000000000,"latency_us":1,"level":"IntraRack"}]})";
  }
  CoordinatorConfig cfg;
  cfg.sqlite_path = ":memory:";
  cfg.topology_file = path("topology.json");
  cfg.jwt_enabled = true;
  cfg.mtls_enabled = true;
  cfg.tls_cert_path = path("server.crt");
  cfg.tls_key_path = path("server.key");
  cfg.tls_ca_path = path("ca.crt");
  cfg.jwt_private_key_path = path("ca.key");
  cfg.jwt_public_key_path = path("ca.pub");
  Coordinator coordinator(cfg);
  ASSERT_TRUE(coordinator.start(false));
  for (const auto *id : {"node-src", "node-dst"}) {
    control::NodeRecord node;
    node.node_id = id;
    node.state = NodeState::ONLINE;
    node.address = id;
    node.total_vram_bytes = kVram;
    node.telemetry.vram_free_bytes = kVram;
    node.link_capacity_bps = 1'000'000'000;
    node.registered_at_ns = now_ns();
    node.last_heartbeat_ns = node.registered_at_ns;
    ASSERT_TRUE(coordinator.register_node(node));
  }
  control::JobRecord job;
  job.job_id = "auth-job";
  job.node_id = "node-src";
  job.state = JobState::RUNNING;
  job.vram_bytes = kCheckpoint;
  job.checkpoint_size = kCheckpoint;
  ASSERT_TRUE(coordinator.register_job(job));
  auto migration = coordinator.on_reclaim_signal("node-src", job.job_id, 60,
                                                 ReclaimType::SCHEDULED);
  ASSERT_TRUE(migration);
  auto tls =
      load_tls_bundle(cfg.tls_cert_path, cfg.tls_key_path, cfg.tls_ca_path);
  ASSERT_TRUE(tls);
  auto credentials = make_server_credentials(tls.value());
  ASSERT_TRUE(credentials);
  CoordinatorServiceImpl service(coordinator);
  grpc::ServerBuilder builder;
  int port = 0;
  builder.AddListeningPort("127.0.0.1:0", credentials.value(), &port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_TRUE(server);
  ASSERT_GT(port, 0);
  const auto private_key = read_pem_file(path("ca.key"));
  ASSERT_TRUE(private_key);
  const auto call = [&](const std::string &node,
                        proto::MigrationProgressRequest request) {
    auto bundle = load_tls_bundle(path(node + ".crt"), path(node + ".key"),
                                  path("ca.crt"));
    if (!bundle)
      return grpc::Status(grpc::StatusCode::INTERNAL, bundle.error().message);
    auto client_credentials = make_client_credentials(bundle.value());
    if (!client_credentials)
      return grpc::Status(grpc::StatusCode::INTERNAL,
                          client_credentials.error().message);
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                       client_credentials.value());
    const auto seconds = now_ns() / 1'000'000'000LL;
    auto token = issue_jwt_rs256({node, node, seconds, seconds + 60},
                                 private_key.value());
    if (!token)
      return grpc::Status(grpc::StatusCode::INTERNAL, token.error().message);
    grpc::ClientContext context;
    set_deadline(context);
    context.AddMetadata("authorization", "Bearer " + token.value());
    proto::MigrationProgressResponse response;
    return proto::CoordinatorService::NewStub(channel)->MigrationProgress(
        &context, request, &response);
  };
  proto::MigrationProgressRequest request;
  request.set_job_id(job.job_id);
  request.set_migration_id(migration.value().migration_id);
  request.set_failed(true);
  request.set_timestamp_ns(now_ns());
  EXPECT_EQ(call("node-other", request).error_code(),
            grpc::StatusCode::PERMISSION_DENIED);
  EXPECT_EQ(coordinator.jobs().get(job.job_id)->state, JobState::MIGRATING);
  request.set_failed(false);
  request.set_bytes_transferred(kCheckpoint / 2);
  request.set_timestamp_ns(now_ns());
  auto status = call("node-src", request);
  ASSERT_TRUE(status.ok()) << status.error_message();
  request.set_bytes_transferred(kCheckpoint);
  request.set_completed(true);
  request.set_timestamp_ns(now_ns());
  status = call("node-src", request);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(coordinator.jobs().get(job.job_id)->node_id, "node-dst");
  status = call("node-src", request);
  EXPECT_TRUE(status.ok()) << status.error_message();
  server->Shutdown();
  coordinator.stop();
#endif
}

TEST_F(GrpcTest, ZeroNoticeEmergencyReportsAcceptNodeLevelTraceEvents) {
  constexpr int64_t trace_ns = 1772302390292933888LL;
  auto source = coordinator->nodes().get("node-src").value();
  source.registered_at_ns = trace_ns - 3600000000000LL;
  source.last_heartbeat_ns = trace_ns - 1;
  ASSERT_TRUE(coordinator->register_node(source));
  proto::ReclaimEvent event;
  event.set_node_id("node-src");
  event.set_type("EMERGENCY");
  event.set_tau_seconds(0.0);
  event.set_timestamp(trace_ns);
  proto::Empty response;
  grpc::ClientContext node_context;
  set_deadline(node_context);
  auto status = stub->ReportReclaim(&node_context, event, &response);
  ASSERT_TRUE(status.ok()) << status.error_message();
  auto hazard = coordinator->hazard().snapshot().at("node-src");
  EXPECT_EQ(hazard.last_event_ns, trace_ns);
  EXPECT_NEAR(coordinator->hazard().estimate_at("node-src", trace_ns).value(),
              1.0 / 3600.0, 1e-12);
  EXPECT_EQ(coordinator->jobs().size(), 0U);

  register_job("emergency-job");
  event.set_job_id("emergency-job");
  event.set_timestamp(trace_ns + 1);
  grpc::ClientContext job_context;
  set_deadline(job_context);
  status = stub->ReportReclaim(&job_context, event, &response);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(coordinator->hazard().snapshot().at("node-src").last_event_ns,
            trace_ns + 1);
  EXPECT_NEAR(
      coordinator->hazard().estimate_at("node-src", trace_ns + 1).value(),
      2.0 / 3600.0, 1e-12);
  EXPECT_EQ(coordinator->jobs().get("emergency-job")->state,
            reclaimnet::JobState::RUNNING);

  event.set_type("SCHEDULED");
  grpc::ClientContext scheduled_context;
  set_deadline(scheduled_context);
  status = stub->ReportReclaim(&scheduled_context, event, &response);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_NEAR(
      coordinator->hazard().estimate_at("node-src", trace_ns + 1).value(),
      2.0 / 3600.0, 1e-12);

  proto::ReclaimSignalRequest signal;
  signal.set_node_id("node-src");
  signal.set_job_id("emergency-job");
  signal.set_reclaim_type(proto::RECLAIM_EMERGENCY);
  signal.set_deadline_s(0.0);
  signal.set_event_timestamp_ns(trace_ns + 2);
  proto::ReclaimSignalResponse signal_response;
  grpc::ClientContext signal_context;
  set_deadline(signal_context);
  status = stub->ReclaimSignal(&signal_context, signal, &signal_response);
  EXPECT_EQ(status.error_code(), grpc::StatusCode::ABORTED);
  EXPECT_FALSE(signal_response.success());
  EXPECT_NEAR(
      coordinator->hazard().estimate_at("node-src", trace_ns + 2).value(),
      3.0 / 3600.0, 1e-12);
  EXPECT_EQ(coordinator->jobs().get("emergency-job")->state,
            reclaimnet::JobState::SUSPENDED);
}

TEST_F(GrpcTest, LegacyMigrationAndReclaimRpcsAreImplemented) {
#if !RECLAIMNET_HAVE_JSON
  GTEST_SKIP() << "JSON topology support is disabled";
#else
  register_job("legacy-migrate");
  proto::MigrationRequest request;
  request.set_job_id("legacy-migrate");
  request.set_src_node_id("node-src");
  request.set_tau_seconds(20.0);
  request.set_checkpoint_size(kCheckpoint);
  proto::MigrationDecision decision;
  grpc::ClientContext context;
  set_deadline(context);
  auto status = stub->RequestMigration(&context, request, &decision);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_TRUE(decision.feasible());
  EXPECT_GT(decision.allocated_bps(), 0U);
  EXPECT_DOUBLE_EQ(decision.exact_allocated_bps(),
                   reclaimnet::bytes_to_bits(kCheckpoint));
  EXPECT_DOUBLE_EQ(decision.exact_allocated_bps(),
                   static_cast<double>(decision.allocated_bps()));
  EXPECT_GT(decision.transfer_time(), 0.0);
  EXPECT_GT(decision.deadline_ns(), reclaimnet::now_ns());
  ASSERT_TRUE(client->migration_progress("legacy-migrate", kCheckpoint, true));

  register_job("legacy-reclaim");
  proto::ReclaimEvent event;
  event.set_node_id("node-src");
  event.set_job_id("legacy-reclaim");
  event.set_tau_seconds(20.0);
  event.set_type("scheduled");
  proto::Empty response;
  grpc::ClientContext event_context;
  set_deadline(event_context);
  status = stub->ReportReclaim(&event_context, event, &response);
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_EQ(coordinator->jobs().get("legacy-reclaim")->state,
            reclaimnet::JobState::RUNNING);
#endif
}
}
