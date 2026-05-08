#include "reclaimnet/trace_writer.h"
#include "reclaimnet/security.h"
#include <filesystem>
#include <fstream>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
namespace reclaimnet::trace {
Writer::Writer(std::string directory, std::string id)
    : directory_(std::move(directory)), run_id_(std::move(id)) {}
VoidResult Writer::snapshot(const std::string &file, const std::string &json) {
  std::lock_guard<std::mutex> lock(mu_);
  std::filesystem::create_directories(directory_);
  auto path = std::filesystem::path(directory_) / file;
  auto temporary = path.string() + ".tmp";
  {
    std::ofstream stream(temporary, std::ios::binary | std::ios::trunc);
    stream << json << '\n';
    if (!stream)
      return err_void("Trace snapshot write failed");
  }
  std::filesystem::rename(temporary, path);
  return ok_void();
}
VoidResult Writer::append(const std::string &file, const std::string &json) {
  std::lock_guard<std::mutex> lock(mu_);
  std::filesystem::create_directories(directory_);
  std::ofstream stream(std::filesystem::path(directory_) / file,
                       std::ios::app | std::ios::binary);
  stream << json << '\n';
  return stream ? ok_void() : err_void("Trace append failed");
}
VoidResult Writer::finalize() {
#if RECLAIMNET_HAVE_JSON
  using J = nlohmann::json;
  J manifest;
  {
    std::ifstream stream(std::filesystem::path(directory_) / "manifest.json");
    stream >> manifest;
  }
  J files = J::object(), counts = J::object();
  for (const auto &entry : std::filesystem::directory_iterator(directory_)) {
    auto name = entry.path().filename().string();
    if (!entry.is_regular_file() || name == "manifest.json" ||
        entry.path().extension() == ".tmp")
      continue;
    auto hash = sha256_file(entry.path().string());
    if (!hash)
      return hash.error();
    files[name] = {{"bytes", entry.file_size()}, {"sha256", hash.value()}};
    if (entry.path().extension() == ".jsonl") {
      std::ifstream stream(entry.path());
      std::string line;
      uint64_t count = 0;
      while (std::getline(stream, line))
        if (!line.empty())
          ++count;
      counts[entry.path().stem().string()] = count;
    }
  }
  manifest["files"] = files;
  manifest["counts"] = counts;
  return snapshot("manifest.json", manifest.dump());
#else
  return err_void("Trace export requires JSON");
#endif
}
VoidResult Writer::initialize(double beta, double loss, double budget,
                              double research, double floor, double epoch,
                              double alpha, size_t k_min, double load,
                              double minimum_transfer, double capacity_factor) {
#if RECLAIMNET_HAVE_JSON
  using J = nlohmann::json;
  J schema = {{"schema_version", 3},
              {"canonical_units",
               {{"*_bytes", "bytes"},
                {"*_bps", "bits_per_second"},
                {"*_ns", "unix_epoch_nanoseconds"},
                {"*_s", "seconds"},
                {"lambda_e", "events_per_second"},
                {"gpu_util_pct", "percent_0_to_100"}}},
              {"enums",
               {{"NodeState",
                 {{"UNKNOWN", 0},
                  {"ONLINE", 1},
                  {"DEGRADED", 2},
                  {"OFFLINE", 3},
                  {"DRAINING", 4}}},
                {"JobState",
                 {{"UNKNOWN", 0},
                  {"RUNNING", 1},
                  {"CHECKPOINTING", 2},
                  {"MIGRATING", 3},
                  {"SUSPENDED", 4},
                  {"COMPLETED", 5},
                  {"FAILED", 6}}},
                {"ReclaimType", {{"SCHEDULED", 0}, {"EMERGENCY", 1}}},
                {"MigrationPhase",
                 {{"IDLE", 0},
                  {"ADMITTED", 1},
                  {"TRANSFERRING", 2},
                  {"RESTORING", 3},
                  {"COMPLETED", 4},
                  {"ABORTED", 5}}},
                {"TopologyLevel",
                 {{"IntraRack", 0},
                  {"IntraBuilding", 1},
                  {"CrossBuilding", 2},
                  {"CoreCrossing", 3}}}}},
              {"topology_cost",
               {{"IntraRack", 0.0},
                {"IntraBuilding", 0.3},
                {"CrossBuilding", 0.7},
                {"CoreCrossing", 1.0}}}};
  if (auto written = snapshot("schema.json", schema.dump()); !written)
    return written;
  return snapshot("manifest.json",
                  J{{"schema_version", 3},
                    {"run_id", run_id_},
                    {"source", "coordinator_export"},
                    {"policy", "reclaimnet"},
                    {"timezone", "Asia/Taipei"},
                    {"parameters",
                     {{"p1_beta", beta},
                      {"p1_loss_budget_s", loss},
                      {"p1_global_budget_bps", budget},
                      {"p1_control_epoch_s", epoch},
                      {"p2_alpha", alpha},
                      {"p2_k_min", k_min},
                      {"p2_load_limit_pct", load},
                      {"p3_beta", research},
                      {"p3_bmin_bps", floor},
                      {"p3_min_useful_transfer_s", minimum_transfer},
                      {"causal_capacity_lower_bound_factor", capacity_factor}}},
                    {"files", J::object()}}
                      .dump());
#else
  return err_void("Trace export requires JSON");
#endif
}
}
