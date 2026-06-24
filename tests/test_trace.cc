#include <gtest/gtest.h>

#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/p3_admission.h"
#include "reclaimnet/trace.h"

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>

#if defined(RECLAIMNET_HAVE_JSON) && RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif

namespace {
namespace trace = reclaimnet::trace;
using reclaimnet::NodeState;
using reclaimnet::ReclaimType;
using reclaimnet::TopologyLevel;
using reclaimnet::control::MigrationEntry;
using reclaimnet::control::P1Allocator;
using reclaimnet::control::P1JobInput;
using reclaimnet::control::P3Admission;

TEST(Trace, ReconstructsUndirectedPathEdges) {
  auto nodes = trace::PathNodes(
      "a", "b", {{"a", "s1"}, {"core", "s1"}, {"core", "s2"}, {"b", "s2"}});
  ASSERT_TRUE(nodes) << nodes.error().message;
  EXPECT_EQ(nodes.value(),
            (std::vector<std::string>{"a", "s1", "core", "s2", "b"}));
  EXPECT_FALSE(trace::PathNodes("a", "b", {{"a", "s1"}, {"s2", "b"}}));
  EXPECT_FALSE(trace::PathNodes("a", "b", {{"a", "s1"}}));
}

#if defined(RECLAIMNET_HAVE_JSON) && RECLAIMNET_HAVE_JSON
using Json = nlohmann::json;
const std::vector<std::string> kRuns{"trace_1", "trace2", "trace3", "trace4",
                                     "trace5"};

std::filesystem::path Fixture(const std::string &run, const std::string &file) {
#ifdef RECLAIMNET_TRACE_FIXTURE_DIR
  return std::filesystem::path(RECLAIMNET_TRACE_FIXTURE_DIR) / run / file;
#else
  return std::filesystem::path(__FILE__).parent_path() / "fixtures" /
         "trace_v3" / run / file;
#endif
}

std::string ReadFile(const std::filesystem::path &path) {
  std::ifstream input(path);
  if (!input)
    throw std::runtime_error("cannot read fixture " + path.string());
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

template <typename T>
std::vector<T> Rows(const std::string &name,
                    const std::string &run = "trace_1") {
  std::istringstream input(ReadFile(Fixture(run, name)));
  std::vector<T> records;
  auto count = trace::ReadJsonLines<T>(input, [&](const T &row) {
    records.push_back(row);
    return reclaimnet::ok_void();
  });
  if (!count)
    throw std::runtime_error(count.error().message);
  return records;
}

template <typename T> void ExpectRoundTrip(const std::string &name) {
  for (const auto &row : Rows<T>(name)) {
    const auto encoded = trace::Serialize(row);
    ASSERT_TRUE(encoded) << encoded.error().message;
    EXPECT_EQ(Json::parse(encoded.value()), Json::parse(row.raw_json));
    EXPECT_TRUE(trace::Parse<T>(encoded.value()));
  }
}

TEST(Trace, ValidatesAllFiveRunSchemasAndManifests) {
  for (const auto &run : kRuns) {
    SCOPED_TRACE(run);
    auto schema =
        trace::Parse<trace::Schema>(ReadFile(Fixture(run, "schema.json")));
    ASSERT_TRUE(schema) << schema.error().message;
    EXPECT_EQ(schema.value().schema_version, 3);
    auto manifest =
        trace::Parse<trace::Manifest>(ReadFile(Fixture(run, "manifest.json")));
    ASSERT_TRUE(manifest) << manifest.error().message;
    EXPECT_EQ(manifest.value().timezone, "Asia/Taipei");
    EXPECT_DOUBLE_EQ(manifest.value().p1_beta, 0.2);
    EXPECT_DOUBLE_EQ(manifest.value().p1_loss_budget_s, 18000.0);
    EXPECT_DOUBLE_EQ(manifest.value().p1_global_budget_bps, 7000000000.0);
    EXPECT_DOUBLE_EQ(manifest.value().p3_beta, 0.3);
    EXPECT_DOUBLE_EQ(manifest.value().p3_bmin_bps, 50000000.0);
    auto encoded = trace::Serialize(manifest.value());
    ASSERT_TRUE(encoded) << encoded.error().message;
    EXPECT_EQ(Json::parse(encoded.value()),
              Json::parse(manifest.value().raw_json));
    auto encoded_schema = trace::Serialize(schema.value());
    ASSERT_TRUE(encoded_schema) << encoded_schema.error().message;
    EXPECT_EQ(Json::parse(encoded_schema.value()),
              Json::parse(schema.value().raw_json));
  }
}

TEST(Trace, RejectsWrongUnitsVersionsAndEnumMappings) {
  auto schema = Json::parse(ReadFile(Fixture("trace_1", "schema.json")));
  schema["canonical_units"]["*_bps"] = "bytes_per_second";
  EXPECT_FALSE(trace::Parse<trace::Schema>(schema.dump()));
  schema = Json::parse(ReadFile(Fixture("trace_1", "schema.json")));
  schema["enums"]["NodeState"]["OFFLINE"] = 2;
  EXPECT_FALSE(trace::Parse<trace::Schema>(schema.dump()));
  schema["schema_version"] = 2;
  EXPECT_FALSE(trace::Parse<trace::Schema>(schema.dump()));
}

TEST(Trace, PreservesExactUnixNanosecondsAndBandwidthBits) {
  const auto bandwidth =
      Rows<trace::BandwidthSample>("bandwidth_samples.jsonl").front();
  EXPECT_EQ(bandwidth.bps, 999553417u);
  EXPECT_EQ(bandwidth.timestamp_ns, INT64_C(1772294439368803072));
  const auto telemetry = Rows<trace::Telemetry>("telemetry.jsonl").front();

  EXPECT_EQ(telemetry.timestamp_ns, INT64_C(1772294417026003901));
  EXPECT_EQ(telemetry.link_tx_bps, 999553417u);
  auto encoded = trace::Serialize(telemetry);
  ASSERT_TRUE(encoded) << encoded.error().message;
  EXPECT_EQ(Json::parse(encoded.value()).at("timestamp").get<int64_t>(),
            telemetry.timestamp_ns);
  auto malformed = Json::parse(telemetry.raw_json);
  malformed["timestamp"] = static_cast<double>(telemetry.timestamp_ns);
  EXPECT_FALSE(trace::Parse<trace::Telemetry>(malformed.dump()));
  malformed["timestamp"] = UINT64_MAX;
  EXPECT_FALSE(trace::Parse<trace::Telemetry>(malformed.dump()));
  malformed["timestamp"] = -1;
  EXPECT_EQ(
      trace::Parse<trace::Telemetry>(malformed.dump()).value().timestamp_ns,
      -1);
  malformed = Json::parse(bandwidth.raw_json);
  malformed["bps"] = 999553417.0;
  EXPECT_FALSE(trace::Parse<trace::BandwidthSample>(malformed.dump()));
}

TEST(Trace, MapsTraceEnumsToCanonicalCppEnums) {
  auto nodes = trace::ParseNodes(ReadFile(Fixture("trace_1", "nodes.json")));
  ASSERT_TRUE(nodes) << nodes.error().message;
  ASSERT_EQ(nodes.value().size(), 53u);
  auto node = nodes.value().front();
  EXPECT_EQ(node.state, NodeState::ONLINE);
  node.state = NodeState::OFFLINE;
  auto encoded = trace::Serialize(node);
  ASSERT_TRUE(encoded) << encoded.error().message;
  EXPECT_EQ(Json::parse(encoded.value())["state"], "OFFLINE");
  auto bad = Json::parse(node.raw_json);
  bad["state"] = "UP";
  EXPECT_FALSE(trace::Parse<trace::Node>(bad.dump()));
  const auto reclaim = Rows<trace::ReclaimEvent>("reclaims.jsonl").front();
  EXPECT_EQ(reclaim.type, ReclaimType::SCHEDULED);
  auto event = Json::parse(reclaim.raw_json);
  event["type"] = "EMERGENCY";
  EXPECT_EQ(trace::Parse<trace::ReclaimEvent>(event.dump()).value().type,
            ReclaimType::EMERGENCY);
  const auto flow = Rows<trace::Flow>("flows.jsonl").front();
  for (const auto &pair : {std::pair{"IntraRack", TopologyLevel::INTRA_RACK},
                           {"IntraBuilding", TopologyLevel::INTRA_BUILDING},
                           {"CrossBuilding", TopologyLevel::CROSS_BUILDING},
                           {"CoreCrossing", TopologyLevel::CORE_CROSSING}}) {
    auto value = Json::parse(flow.raw_json);
    value["topology_level"] = pair.first;
    EXPECT_EQ(trace::Parse<trace::Flow>(value.dump()).value().topology_level,
              pair.second);
  }
  EXPECT_EQ(
      trace::Parse<trace::Job>(Rows<trace::Job>("jobs.jsonl").front().raw_json)
          .value()
          .state,
      reclaimnet::JobState::SUSPENDED);
}

TEST(Trace, RoundTripsRealRecordsAndAdditionalFields) {
  ExpectRoundTrip<trace::BandwidthSample>("bandwidth_samples.jsonl");
  ExpectRoundTrip<trace::HazardSample>("hazard_samples.jsonl");
  ExpectRoundTrip<trace::ReclaimEvent>("reclaims.jsonl");
  ExpectRoundTrip<trace::Telemetry>("telemetry.jsonl");
  ExpectRoundTrip<trace::Job>("jobs.jsonl");
  ExpectRoundTrip<trace::Flow>("flows.jsonl");
  ExpectRoundTrip<trace::Migration>("migrations.jsonl");
  for (const auto &run : kRuns) {
    auto nodes = trace::ParseNodes(ReadFile(Fixture(run, "nodes.json")));
    ASSERT_TRUE(nodes) << nodes.error().message;
    auto encoded_nodes = trace::SerializeNodes(nodes.value());
    ASSERT_TRUE(encoded_nodes) << encoded_nodes.error().message;
    EXPECT_EQ(Json::parse(encoded_nodes.value()),
              Json::parse(ReadFile(Fixture(run, "nodes.json"))));
    for (const auto &epoch :
         Rows<trace::P1Epoch>("p1_allocation_epochs.jsonl", run)) {
      auto encoded = trace::Serialize(epoch);
      ASSERT_TRUE(encoded) << encoded.error().message;
      EXPECT_EQ(Json::parse(encoded.value()), Json::parse(epoch.raw_json));
    }
  }
  auto source =
      Json::parse(Rows<trace::Telemetry>("telemetry.jsonl").front().raw_json);
  source["extension"] = {{"capture_ns", INT64_C(1772294417026003901)},
                         {"annotation", "preserved"}};
  auto parsed = trace::Parse<trace::Telemetry>(source.dump());
  ASSERT_TRUE(parsed) << parsed.error().message;
  parsed.value().gpu_util_pct = 40.0;
  auto encoded = trace::Serialize(parsed.value());
  ASSERT_TRUE(encoded) << encoded.error().message;
  EXPECT_EQ(Json::parse(encoded.value())["extension"], source["extension"]);
  EXPECT_DOUBLE_EQ(Json::parse(encoded.value())["gpu_util_pct"].get<double>(),
                   40.0);
}

TEST(Trace, JsonLinesReportsMalformedLineAndConsumerErrors) {
  const auto row =
      Rows<trace::HazardSample>("hazard_samples.jsonl").front().raw_json;
  std::istringstream input("\n" + row + "\ninvalid\n" + row + "\n");
  size_t seen = 0;
  auto count = trace::ReadJsonLines<trace::HazardSample>(
      input, [&](const trace::HazardSample &) {
        ++seen;
        return reclaimnet::ok_void();
      });
  EXPECT_FALSE(count);
  EXPECT_EQ(seen, 1u);
  EXPECT_NE(count.error().message.find("line 3"), std::string::npos);
  std::istringstream rejected(row + "\n");
  auto result = trace::ReadJsonLines<trace::HazardSample>(
      rejected, [](const trace::HazardSample &) {
        return reclaimnet::err_void("consumer refused input");
      });
  EXPECT_FALSE(result);
  EXPECT_NE(result.error().message.find("consumer refused input"),
            std::string::npos);
}

TEST(Trace, RunsP1AllocatorAgainstRealEpochsFromAllFiveRuns) {
  size_t checked = 0;
  for (const auto &run : kRuns) {
    auto manifest =
        trace::Parse<trace::Manifest>(ReadFile(Fixture(run, "manifest.json")));
    ASSERT_TRUE(manifest) << manifest.error().message;
    for (const auto &epoch :
         Rows<trace::P1Epoch>("p1_allocation_epochs.jsonl", run)) {
      SCOPED_TRACE(run + ":" + epoch.allocation_epoch_id);
      const auto input = trace::ToP1Inputs<P1JobInput>(
          epoch, manifest.value().p1_loss_budget_s);
      const auto result =
          P1Allocator(epoch.allocation_budget_bps).compute(input);
      ASSERT_EQ(result.schedules.size(), epoch.members.size());
      EXPECT_NEAR(result.total_bps_used, epoch.total_allocated_bps,
                  std::max(1.0, epoch.total_allocated_bps) * 1e-12);
      for (size_t i = 0; i < epoch.members.size(); ++i) {
        const auto &expected = epoch.members[i];
        const auto &actual = result.schedules[i];
        EXPECT_EQ(actual.job_id, expected.job_id);
        EXPECT_EQ(actual.feasible, expected.constraints_feasible);
        EXPECT_NEAR(actual.b_i_bps, expected.b_i_bps,
                    std::max(1.0, expected.b_i_bps) * 1e-12);
        EXPECT_NEAR(actual.write_rate_limit_bps, expected.write_rate_limit_bps,
                    std::max(1.0, expected.write_rate_limit_bps) * 1e-12);
        EXPECT_NEAR(actual.raw_delta_t_s, expected.p1_raw_interval_s,
                    expected.p1_raw_interval_s * 1e-12);
        EXPECT_NEAR(actual.lower_bound_s, expected.p1_lower_interval_s,
                    expected.p1_lower_interval_s * 1e-12);
        EXPECT_DOUBLE_EQ(actual.upper_bound_s, expected.p1_upper_interval_s);
        EXPECT_NEAR(actual.delta_t_s, expected.target_interval_s,
                    expected.target_interval_s * 1e-12);
        ++checked;
      }
    }
  }
  EXPECT_EQ(checked, 20u);
}

TEST(Trace, FeedsScheduledRankStateIntoP3AndPreservesDeadlineExactly) {
  const auto records = Rows<trace::Migration>("migrations.jsonl");
  ASSERT_GE(records.size(), 2u);
  const auto &migration = records.front();
  auto input = trace::ToP3Entry<MigrationEntry>(migration);
  ASSERT_TRUE(input) << input.error().message;
  EXPECT_EQ(input.value().data_total_bytes, 109057577u);
  EXPECT_EQ(input.value().start_ns, INT64_C(1772302390292933888));
  EXPECT_EQ(input.value().deadline_ns, INT64_C(1772302665327266888));
  EXPECT_DOUBLE_EQ(input.value().deadline_s, 275.034333);
  EXPECT_NEAR(input.value().deadline_s - input.value().restore_time_s,
              *migration.available_transfer_s, 1e-12);
  P3Admission admission(0.7, 50000000.0);
  const auto raw = Json::parse(migration.raw_json);
  for (const auto &evidence : raw.at("reservation_evidence"))
    admission.add_link(evidence["edge"][0].get<std::string>(),
                       evidence["edge"][1].get<std::string>(),
                       evidence["samples"][0]["capacity_bps"].get<double>());
  auto admitted = admission.admit(input.value());
  ASSERT_TRUE(admitted) << admitted.error().message;
  EXPECT_NEAR(admitted.value().allocated_bps,
              migration.path_allocations.front().required_bps, 1e-6);
  EXPECT_EQ(admitted.value().deadline_ns, *migration.deadline_ns);
  EXPECT_TRUE(admission.expired_migrations(*migration.deadline_ns - 1).empty());
  EXPECT_EQ(admission.expired_migrations(*migration.deadline_ns).size(), 1u);
  EXPECT_EQ(records.back().reclaim_type, ReclaimType::EMERGENCY);
  EXPECT_FALSE(trace::ToP3Entry<MigrationEntry>(records.back()));
}

#else
TEST(Trace, MissingJsonDependencyReturnsExplicitErrors) {
  auto parsed = trace::Parse<trace::BandwidthSample>(
      R"({"node_id":"a","bps":1,"timestamp":1})");
  EXPECT_FALSE(parsed);
  EXPECT_NE(parsed.error().message.find("nlohmann_json"), std::string::npos);
  EXPECT_FALSE(trace::Serialize(trace::BandwidthSample{}));
  EXPECT_FALSE(trace::ParseNodes("[]"));
  EXPECT_FALSE(trace::SerializeNodes({}));
}
#endif

}
