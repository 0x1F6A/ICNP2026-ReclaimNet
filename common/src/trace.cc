#include "reclaimnet/trace.h"

#include <cmath>
#include <limits>
#include <stdexcept>

#if defined(RECLAIMNET_HAVE_JSON) && RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif

namespace reclaimnet::trace {

Result<std::vector<NodeId>> PathNodes(const NodeId &src, const NodeId &dst,
                                      const std::vector<Edge> &edges) {
  if (src.empty() || dst.empty())
    return Error{"path endpoints must be nonempty"};
  std::vector<NodeId> nodes{src};
  for (const auto &edge : edges) {
    if (edge.first == nodes.back())
      nodes.push_back(edge.second);
    else if (edge.second == nodes.back())
      nodes.push_back(edge.first);
    else
      return Error{"path edges are not contiguous from source"};
    if (nodes.back().empty())
      return Error{"path node must be nonempty"};
  }
  if (nodes.back() != dst)
    return Error{"path does not terminate at destination"};
  return nodes;
}

#if defined(RECLAIMNET_HAVE_JSON) && RECLAIMNET_HAVE_JSON
namespace {
using Json = nlohmann::json;

void Require(bool condition, const std::string &message) {
  if (!condition)
    throw std::invalid_argument(message);
}

Json Object(std::string_view text) {
  auto j = Json::parse(text);
  Require(j.is_object(), "trace record must be a JSON object");
  return j;
}

std::string Str(const Json &j, const char *key) {
  Require(j.at(key).is_string(), std::string(key) + " must be a string");
  return j.at(key).get<std::string>();
}

uint64_t UInt(const Json &j, const char *key) {
  const auto &n = j.at(key);
  Require(n.is_number_integer() || n.is_number_unsigned(),
          std::string(key) + " must be an exact integer");
  if (n.is_number_unsigned())
    return n.get<uint64_t>();
  auto value = n.get<int64_t>();
  Require(value >= 0, std::string(key) + " must be nonnegative");
  return static_cast<uint64_t>(value);
}

int64_t Ns(const Json &j, const char *key) {
  const auto &n = j.at(key);
  Require(n.is_number_integer() || n.is_number_unsigned(),
          std::string(key) + " must be exact integer nanoseconds");
  if (!n.is_number_unsigned())
    return n.get<int64_t>();
  auto value = n.get<uint64_t>();
  Require(value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
          std::string(key) + " is outside int64 nanoseconds");
  return static_cast<int64_t>(value);
}

int Int(const Json &j, const char *key) {
  auto value = UInt(j, key);
  Require(value <= static_cast<uint64_t>(std::numeric_limits<int>::max()),
          std::string(key) + " is outside int range");
  return static_cast<int>(value);
}

double Num(const Json &j, const char *key) {
  Require(j.at(key).is_number(), std::string(key) + " must be numeric");
  double value = j.at(key).get<double>();
  Require(std::isfinite(value) && value >= 0,
          std::string(key) + " must be finite and nonnegative");
  return value;
}

bool Bool(const Json &j, const char *key) {
  Require(j.at(key).is_boolean(), std::string(key) + " must be boolean");
  return j.at(key).get<bool>();
}

template <typename T, typename Read>
std::optional<T> Optional(const Json &j, const char *key, Read read) {
  if (!j.contains(key) || j.at(key).is_null())
    return std::nullopt;
  return read(j, key);
}

template <typename T>
void PutOptional(Json &j, const char *key, const std::optional<T> &value) {
  if (value)
    j[key] = *value;
  else if (j.contains(key))
    j[key] = nullptr;
}

std::vector<std::string> Strings(const Json &j, const char *key) {
  Require(j.at(key).is_array(), std::string(key) + " must be an array");
  std::vector<std::string> result;
  for (const auto &v : j.at(key)) {
    Require(v.is_string(), std::string(key) + " elements must be strings");
    result.push_back(v.get<std::string>());
  }
  return result;
}

std::vector<Edge> Edges(const Json &j) {
  const auto &values = j.at("path_edges");
  Require(values.is_array(), "path_edges must be an array");
  std::vector<Edge> result;
  for (const auto &edge : values) {
    Require(edge.is_array() && edge.size() == 2 && edge[0].is_string() &&
                edge[1].is_string(),
            "path edge must contain two node strings");
    result.emplace_back(edge[0].get<std::string>(), edge[1].get<std::string>());
  }
  return result;
}

Json EdgeJson(const std::vector<Edge> &edges) {
  auto j = Json::array();
  for (const auto &e : edges)
    j.push_back(Json::array({e.first, e.second}));
  return j;
}

template <typename Enum>
Enum EnumValue(const Json &j, const char *key,
               std::initializer_list<std::pair<const char *, Enum>> names) {
  const auto &value = j.at(key);
  for (const auto &item : names) {
    if (value.is_string() && value.get<std::string>() == item.first)
      return item.second;
    if ((value.is_number_integer() || value.is_number_unsigned()) &&
        value == static_cast<unsigned>(item.second))
      return item.second;
  }
  throw std::invalid_argument(std::string("unknown ") + key + " enum value");
}

template <typename Enum>
std::string
EnumName(Enum value,
         std::initializer_list<std::pair<const char *, Enum>> names) {
  for (const auto &item : names)
    if (item.second == value)
      return item.first;
  throw std::invalid_argument("unknown enum value");
}

#define NODE_NAMES                                                             \
  {                                                                            \
    {"UNKNOWN", NodeState::UNKNOWN}, {"ONLINE", NodeState::ONLINE},            \
        {"DEGRADED", NodeState::DEGRADED}, {"OFFLINE", NodeState::OFFLINE}, {  \
      "DRAINING", NodeState::DRAINING                                          \
    }                                                                          \
  }
#define JOB_NAMES                                                              \
  {                                                                            \
    {"UNKNOWN", JobState::UNKNOWN}, {"RUNNING", JobState::RUNNING},            \
        {"CHECKPOINTING", JobState::CHECKPOINTING},                            \
        {"MIGRATING", JobState::MIGRATING},                                    \
        {"SUSPENDED", JobState::SUSPENDED},                                    \
        {"COMPLETED", JobState::COMPLETED}, {                                  \
      "FAILED", JobState::FAILED                                               \
    }                                                                          \
  }
#define RECLAIM_NAMES                                                          \
  {                                                                            \
    {"SCHEDULED", ReclaimType::SCHEDULED}, {                                   \
      "EMERGENCY", ReclaimType::EMERGENCY                                      \
    }                                                                          \
  }
#define PHASE_NAMES                                                            \
  {                                                                            \
    {"IDLE", MigrationPhase::IDLE}, {"ADMITTED", MigrationPhase::ADMITTED},    \
        {"TRANSFERRING", MigrationPhase::TRANSFERRING},                        \
        {"RESTORING", MigrationPhase::RESTORING},                              \
        {"COMPLETED", MigrationPhase::COMPLETED}, {                            \
      "ABORTED", MigrationPhase::ABORTED                                       \
    }                                                                          \
  }
#define TOPO_NAMES                                                             \
  {                                                                            \
    {"IntraRack", TopologyLevel::INTRA_RACK},                                  \
        {"IntraBuilding", TopologyLevel::INTRA_BUILDING},                      \
        {"CrossBuilding", TopologyLevel::CROSS_BUILDING}, {                    \
      "CoreCrossing", TopologyLevel::CORE_CROSSING                             \
    }                                                                          \
  }

template <typename Enum>
void CheckEnum(const Json &j, const char *name,
               std::initializer_list<std::pair<const char *, Enum>> values) {
  const auto &mapping = j.at("enums").at(name);
  for (const auto &item : values)
    Require(UInt(mapping, item.first) == static_cast<uint64_t>(item.second),
            std::string("incompatible ") + name + " enum mapping");
}

void Decode(const Json &j, Schema &out) {
  out.schema_version = Int(j, "schema_version");
  Require(out.schema_version == 3, "only trace schema_version 3 is supported");
  const auto &units = j.at("canonical_units");
  for (const auto &unit : {std::pair{"*_bytes", "bytes"},
                           {"*_bps", "bits_per_second"},
                           {"*_ns", "unix_epoch_nanoseconds"},
                           {"*_s", "seconds"},
                           {"lambda_e", "events_per_second"},
                           {"gpu_util_pct", "percent_0_to_100"}})
    Require(Str(units, unit.first) == unit.second,
            std::string("incompatible canonical unit: ") + unit.first);
  CheckEnum<NodeState>(j, "NodeState", NODE_NAMES);
  CheckEnum<JobState>(j, "JobState", JOB_NAMES);
  CheckEnum<ReclaimType>(j, "ReclaimType", RECLAIM_NAMES);
  CheckEnum<MigrationPhase>(j, "MigrationPhase", PHASE_NAMES);
  CheckEnum<TopologyLevel>(j, "TopologyLevel", TOPO_NAMES);
}

Json Encode(const Schema &r) {
  auto j = Object(r.raw_json);
  j["schema_version"] = r.schema_version;
  j["canonical_units"] = {{"*_bytes", "bytes"},
                          {"*_bps", "bits_per_second"},
                          {"*_ns", "unix_epoch_nanoseconds"},
                          {"*_s", "seconds"},
                          {"lambda_e", "events_per_second"},
                          {"gpu_util_pct", "percent_0_to_100"}};
  j["enums"]["NodeState"] = {{"UNKNOWN", 0},
                             {"ONLINE", 1},
                             {"DEGRADED", 2},
                             {"OFFLINE", 3},
                             {"DRAINING", 4}};
  j["enums"]["JobState"] = {{"UNKNOWN", 0},       {"RUNNING", 1},
                            {"CHECKPOINTING", 2}, {"MIGRATING", 3},
                            {"SUSPENDED", 4},     {"COMPLETED", 5},
                            {"FAILED", 6}};
  j["enums"]["ReclaimType"] = {{"SCHEDULED", 0}, {"EMERGENCY", 1}};
  j["enums"]["MigrationPhase"] = {{"IDLE", 0},         {"ADMITTED", 1},
                                  {"TRANSFERRING", 2}, {"RESTORING", 3},
                                  {"COMPLETED", 4},    {"ABORTED", 5}};
  j["enums"]["TopologyLevel"] = {{"IntraRack", 0},
                                 {"IntraBuilding", 1},
                                 {"CrossBuilding", 2},
                                 {"CoreCrossing", 3}};
  return j;
}

void Decode(const Json &j, Manifest &r) {
  r.schema_version = Int(j, "schema_version");
  Require(r.schema_version == 3, "only trace schema_version 3 is supported");
  r.run_id = Str(j, "run_id");
  r.timezone = Str(j, "timezone");
  const auto &p = j.at("parameters");
  r.p1_beta = Num(p, "p1_beta");
  r.p1_loss_budget_s = Num(p, "p1_loss_budget_s");
  r.p1_global_budget_bps = Num(p, "p1_global_budget_bps");
  r.p3_beta = Num(p, "p3_beta");
  r.p3_bmin_bps = Num(p, "p3_bmin_bps");
  r.p3_min_useful_transfer_s = Num(p, "p3_min_useful_transfer_s");
  Require(r.p1_beta > 0 && r.p1_beta <= 1 && r.p3_beta <= 1 &&
              r.p1_loss_budget_s > 0,
          "invalid trace policy parameters");
}
Json Encode(const Manifest &r) {
  auto j = Object(r.raw_json);
  j["schema_version"] = r.schema_version;
  j["run_id"] = r.run_id;
  j["timezone"] = r.timezone;
  auto &p = j["parameters"];
  p["p1_beta"] = r.p1_beta;
  p["p1_loss_budget_s"] = r.p1_loss_budget_s;
  p["p1_global_budget_bps"] = r.p1_global_budget_bps;
  p["p3_beta"] = r.p3_beta;
  p["p3_bmin_bps"] = r.p3_bmin_bps;
  p["p3_min_useful_transfer_s"] = r.p3_min_useful_transfer_s;
  return j;
}

void Decode(const Json &j, BandwidthSample &r) {
  r.node_id = Str(j, "node_id");
  r.bps = UInt(j, "bps");
  r.timestamp_ns = Ns(j, "timestamp");
}
Json Encode(const BandwidthSample &r) {
  auto j = Object(r.raw_json);
  j["node_id"] = r.node_id;
  j["bps"] = r.bps;
  j["timestamp"] = r.timestamp_ns;
  return j;
}
void Decode(const Json &j, HazardSample &r) {
  r.node_id = Str(j, "node_id");
  r.lambda_e = Num(j, "lambda_e");
  r.timestamp_ns = Ns(j, "timestamp");
}
Json Encode(const HazardSample &r) {
  auto j = Object(r.raw_json);
  j["node_id"] = r.node_id;
  j["lambda_e"] = r.lambda_e;
  j["timestamp"] = r.timestamp_ns;
  return j;
}
void Decode(const Json &j, ReclaimEvent &r) {
  r.node_id = Str(j, "node_id");
  r.type = EnumValue<ReclaimType>(j, "type", RECLAIM_NAMES);
  r.tau_seconds = Num(j, "tau_seconds");
  r.timestamp_ns = Ns(j, "timestamp");
}
Json Encode(const ReclaimEvent &r) {
  auto j = Object(r.raw_json);
  j["node_id"] = r.node_id;
  j["type"] = EnumName<ReclaimType>(r.type, RECLAIM_NAMES);
  j["tau_seconds"] = r.tau_seconds;
  j["timestamp"] = r.timestamp_ns;
  return j;
}

void Decode(const Json &j, P1Member &r) {
  r.job_id = Str(j, "job_id");
  r.source_node_ids = Strings(j, "source_node_ids");
  r.replica_node_ids = Strings(j, "replica_node_ids");
  r.checkpoint_size_bytes = UInt(j, "checkpoint_size_bytes");
  r.lambda_e = Num(j, "lambda_e");
  r.allocation_weight = Num(j, "allocation_weight");
  r.write_cap_bps = Num(j, "write_cap_bps");
  r.b_i_bps = Num(j, "b_i_bps");
  r.write_rate_limit_bps = Num(j, "write_rate_limit_bps");
  r.p1_raw_interval_s = Num(j, "p1_raw_interval_s");
  r.p1_lower_interval_s = Num(j, "p1_lower_interval_s");
  r.p1_upper_interval_s = Num(j, "p1_upper_interval_s");
  r.target_interval_s = Num(j, "target_interval_s");
  r.constraints_feasible = Bool(j, "constraints_feasible");
}
Json Encode(const P1Member &r) {
  auto j = Object(r.raw_json);
  j["job_id"] = r.job_id;
  j["source_node_ids"] = r.source_node_ids;
  j["replica_node_ids"] = r.replica_node_ids;
  j["checkpoint_size_bytes"] = r.checkpoint_size_bytes;
  j["lambda_e"] = r.lambda_e;
  j["allocation_weight"] = r.allocation_weight;
  j["write_cap_bps"] = r.write_cap_bps;
  j["b_i_bps"] = r.b_i_bps;
  j["write_rate_limit_bps"] = r.write_rate_limit_bps;
  j["p1_raw_interval_s"] = r.p1_raw_interval_s;
  j["p1_lower_interval_s"] = r.p1_lower_interval_s;
  j["p1_upper_interval_s"] = r.p1_upper_interval_s;
  j["target_interval_s"] = r.target_interval_s;
  j["constraints_feasible"] = r.constraints_feasible;
  if (j.contains("allocation_slice_bps"))
    j["allocation_slice_bps"] = r.b_i_bps;
  return j;
}
void Decode(const Json &j, P1Epoch &r) {
  r.run_id = Str(j, "run_id");
  r.allocation_epoch_id = Str(j, "allocation_epoch_id");
  r.start_ns = Ns(j, "start_ns");
  r.end_ns = Ns(j, "end_ns");
  Require(r.end_ns >= r.start_ns, "allocation epoch ends before it starts");
  r.control_epoch_s = Num(j, "control_epoch_s");
  r.allocation_budget_bps = Num(j, "allocation_budget_bps");
  r.controller_ceiling_bps = Num(j, "controller_ceiling_bps");
  r.beta = Num(j, "beta");
  Require(r.beta > 0 && r.beta <= 1, "epoch beta must be in (0, 1]");
  r.total_allocated_bps = Num(j, "total_allocated_bps");
  r.water_fill_iterations = Int(j, "water_fill_iterations");
  Require(j.at("members").is_array(), "members must be an array");
  for (const auto &value : j.at("members")) {
    P1Member member;
    member.raw_json = value.dump();
    Decode(value, member);
    r.members.push_back(std::move(member));
  }
}
Json Encode(const P1Epoch &r) {
  auto j = Object(r.raw_json);
  j["run_id"] = r.run_id;
  j["allocation_epoch_id"] = r.allocation_epoch_id;
  j["start_ns"] = r.start_ns;
  j["end_ns"] = r.end_ns;
  if (j.contains("allocation_epoch_start_ns"))
    j["allocation_epoch_start_ns"] = r.start_ns;
  if (j.contains("allocation_epoch_end_ns"))
    j["allocation_epoch_end_ns"] = r.end_ns;
  j["control_epoch_s"] = r.control_epoch_s;
  j["allocation_budget_bps"] = r.allocation_budget_bps;
  j["controller_ceiling_bps"] = r.controller_ceiling_bps;
  j["beta"] = r.beta;
  j["total_allocated_bps"] = r.total_allocated_bps;
  j["water_fill_iterations"] = r.water_fill_iterations;
  j["members"] = Json::array();
  for (const auto &member : r.members)
    j["members"].push_back(Encode(member));
  if (j.contains("active_member_count"))
    j["active_member_count"] = r.members.size();
  return j;
}

void Decode(const Json &j, Node &r) {
  r.node_id = Str(j, "node_id");
  r.state = EnumValue<NodeState>(j, "state", NODE_NAMES);
  r.address = Str(j, "address");
  r.building = Str(j, "building");
  r.switch_id = Str(j, "switch_id");
  r.total_vram_bytes = UInt(j, "total_vram_bytes");
  r.link_capacity_bps = UInt(j, "link_capacity_bps");
  r.registered_at_ns = Ns(j, "registered_at_ns");
  r.last_heartbeat_ns = Ns(j, "last_heartbeat_ns");
}
Json Encode(const Node &r) {
  auto j = Object(r.raw_json);
  j["node_id"] = r.node_id;
  j["state"] = EnumName<NodeState>(r.state, NODE_NAMES);
  j["address"] = r.address;
  j["building"] = r.building;
  j["switch_id"] = r.switch_id;
  j["total_vram_bytes"] = r.total_vram_bytes;
  j["link_capacity_bps"] = r.link_capacity_bps;
  j["registered_at_ns"] = r.registered_at_ns;
  j["last_heartbeat_ns"] = r.last_heartbeat_ns;
  if (j.contains("total_vram"))
    j["total_vram"] = r.total_vram_bytes;
  if (j.contains("link_cap_bps"))
    j["link_cap_bps"] = r.link_capacity_bps;
  if (j.contains("registered_at"))
    j["registered_at"] = r.registered_at_ns;
  return j;
}
void Decode(const Json &j, Telemetry &r) {
  r.node_id = Str(j, "node_id");
  r.gpu_util_pct = Num(j, "gpu_util_pct");
  r.vram_used_pct = Num(j, "vram_used_pct");
  Require(r.gpu_util_pct <= 100 && r.vram_used_pct <= 100,
          "telemetry percentages must be in [0, 100]");
  r.vram_free_bytes = UInt(j, "vram_free_bytes");
  r.link_tx_bps = UInt(j, "link_tx_bps");
  r.link_rx_bps = UInt(j, "link_rx_bps");
  r.cpu_load_1m = Num(j, "cpu_load_1m");
  r.timestamp_ns = Ns(j, "timestamp");
}
Json Encode(const Telemetry &r) {
  auto j = Object(r.raw_json);
  j["node_id"] = r.node_id;
  j["gpu_util_pct"] = r.gpu_util_pct;
  j["vram_used_pct"] = r.vram_used_pct;
  j["vram_free_bytes"] = r.vram_free_bytes;
  j["link_tx_bps"] = r.link_tx_bps;
  j["link_rx_bps"] = r.link_rx_bps;
  j["cpu_load_1m"] = r.cpu_load_1m;
  j["timestamp"] = r.timestamp_ns;
  return j;
}
void Decode(const Json &j, Job &r) {
  r.job_id = Str(j, "job_id");
  r.state = EnumValue<JobState>(j, "state", JOB_NAMES);
  r.node_ids = Strings(j, "node_ids");
  r.logical_checkpoint_size_bytes = UInt(j, "logical_checkpoint_size_bytes");
  r.per_rank_checkpoint_size_bytes = UInt(j, "per_rank_checkpoint_size_bytes");
  r.logical_vram_bytes = UInt(j, "logical_vram_bytes");
  r.lambda_i = Num(j, "lambda_i");
  r.loss_budget_s = Num(j, "loss_budget_s");
  r.submit_time_ns = Ns(j, "submit_time_ns");
  r.last_update_ns = Ns(j, "last_update_ns");
}
Json Encode(const Job &r) {
  auto j = Object(r.raw_json);
  j["job_id"] = r.job_id;
  j["state"] = EnumName<JobState>(r.state, JOB_NAMES);
  j["node_ids"] = r.node_ids;
  j["logical_checkpoint_size_bytes"] = r.logical_checkpoint_size_bytes;
  j["per_rank_checkpoint_size_bytes"] = r.per_rank_checkpoint_size_bytes;
  j["logical_vram_bytes"] = r.logical_vram_bytes;
  j["lambda_i"] = r.lambda_i;
  j["loss_budget_s"] = r.loss_budget_s;
  j["submit_time_ns"] = r.submit_time_ns;
  j["last_update_ns"] = r.last_update_ns;
  return j;
}

template <typename T> T NarrowUInt(const Json &j, const char *key) {
  auto value = UInt(j, key);
  Require(value <= std::numeric_limits<T>::max(),
          std::string(key) + " overflows its wire type");
  return static_cast<T>(value);
}
void Decode(const Json &j, Flow &r) {
  r.flow_id = Str(j, "flow_id");
  r.flow_class = Str(j, "flow_class");
  r.job_id = Str(j, "job_id");
  r.src_node_id = Str(j, "src_node_id");
  r.dst_node_id = Str(j, "dst_node_id");
  r.tuple.src_ip = NarrowUInt<uint32_t>(j, "src_ip");
  r.tuple.dst_ip = NarrowUInt<uint32_t>(j, "dst_ip");
  r.tuple.src_port = NarrowUInt<uint16_t>(j, "src_port");
  r.tuple.dst_port = NarrowUInt<uint16_t>(j, "dst_port");
  r.tuple.protocol = NarrowUInt<uint8_t>(j, "protocol");
  r.bytes = UInt(j, "bytes");
  r.planned_bytes = Optional<uint64_t>(j, "planned_bytes", UInt);
  r.ts_ns = Ns(j, "ts_ns");
  r.end_ns = Ns(j, "end_ns");
  Require(r.end_ns >= r.ts_ns, "flow ends before it starts");
  r.average_rate_bps = Optional<double>(j, "average_rate_bps", Num);
  r.rate_bps = Optional<double>(j, "rate_bps", Num);
  r.required_bps = Optional<double>(j, "required_bps", Num);
  r.topology_level = EnumValue<TopologyLevel>(j, "topology_level", TOPO_NAMES);
  r.path_edges = Edges(j);
}
Json Encode(const Flow &r) {
  auto j = Object(r.raw_json);
  j["flow_id"] = r.flow_id;
  j["flow_class"] = r.flow_class;
  j["job_id"] = r.job_id;
  j["src_node_id"] = r.src_node_id;
  j["dst_node_id"] = r.dst_node_id;
  j["src_ip"] = r.tuple.src_ip;
  j["dst_ip"] = r.tuple.dst_ip;
  j["src_port"] = r.tuple.src_port;
  j["dst_port"] = r.tuple.dst_port;
  j["protocol"] = r.tuple.protocol;
  j["bytes"] = r.bytes;
  PutOptional(j, "planned_bytes", r.planned_bytes);
  PutOptional(j, "average_rate_bps", r.average_rate_bps);
  PutOptional(j, "rate_bps", r.rate_bps);
  PutOptional(j, "required_bps", r.required_bps);
  j["ts_ns"] = r.ts_ns;
  j["end_ns"] = r.end_ns;
  j["topology_level"] = EnumName<TopologyLevel>(r.topology_level, TOPO_NAMES);
  j["path_edges"] = EdgeJson(r.path_edges);
  return j;
}
void Decode(const Json &j, PathAllocation &r) {
  r.rank = Int(j, "rank");
  r.src_node_id = Str(j, "src_node_id");
  r.dst_node_id = Str(j, "dst_node_id");
  r.bytes = UInt(j, "bytes");
  r.required_bps = Num(j, "required_bps");
  r.allocated_bps = Num(j, "allocated_bps");
  r.path_edges = Edges(j);
}
Json Encode(const PathAllocation &r) {
  auto j = Object(r.raw_json);
  j["rank"] = r.rank;
  j["src_node_id"] = r.src_node_id;
  j["dst_node_id"] = r.dst_node_id;
  j["bytes"] = r.bytes;
  j["required_bps"] = r.required_bps;
  j["allocated_bps"] = r.allocated_bps;
  j["path_edges"] = EdgeJson(r.path_edges);
  return j;
}
void Decode(const Json &j, Migration &r) {
  r.migration_id = Str(j, "migration_id");
  r.job_id = Str(j, "job_id");
  r.src_node_id = Str(j, "src_node_id");
  r.dst_node_id = Optional<NodeId>(j, "dst_node_id", Str);
  r.reclaim_type = EnumValue<ReclaimType>(j, "reclaim_type", RECLAIM_NAMES);
  r.phase = EnumValue<MigrationPhase>(j, "phase", PHASE_NAMES);
  r.topology_level = EnumValue<TopologyLevel>(j, "topology_level", TOPO_NAMES);
  r.p3_applicable = Bool(j, "p3_applicable");
  r.p3_status = Str(j, "p3_status");
  r.bytes_to_transfer_total = UInt(j, "bytes_to_transfer_total");
  r.deadline_s = Num(j, "deadline_s");
  r.deadline_ns = Optional<int64_t>(j, "deadline_ns", Ns);
  r.decision_ns = Ns(j, "decision_ns");
  r.start_ns = Ns(j, "start_ns");
  r.end_ns = Ns(j, "end_ns");
  Require(r.end_ns >= r.start_ns, "migration ends before it starts");
  r.restore_time_s = Num(j, "restore_time_s");
  r.available_transfer_s = Optional<double>(j, "available_transfer_s", Num);
  Require(j.at("path_allocations").is_array(),
          "path_allocations must be an array");
  for (const auto &value : j.at("path_allocations")) {
    PathAllocation p;
    p.raw_json = value.dump();
    Decode(value, p);
    r.path_allocations.push_back(std::move(p));
  }
}
Json Encode(const Migration &r) {
  auto j = Object(r.raw_json);
  j["migration_id"] = r.migration_id;
  j["job_id"] = r.job_id;
  j["src_node_id"] = r.src_node_id;
  PutOptional(j, "dst_node_id", r.dst_node_id);
  j["reclaim_type"] = EnumName<ReclaimType>(r.reclaim_type, RECLAIM_NAMES);
  j["phase"] = EnumName<MigrationPhase>(r.phase, PHASE_NAMES);
  j["topology_level"] = EnumName<TopologyLevel>(r.topology_level, TOPO_NAMES);
  j["p3_applicable"] = r.p3_applicable;
  j["p3_status"] = r.p3_status;
  j["bytes_to_transfer_total"] = r.bytes_to_transfer_total;
  j["deadline_s"] = r.deadline_s;
  PutOptional(j, "deadline_ns", r.deadline_ns);
  j["decision_ns"] = r.decision_ns;
  j["start_ns"] = r.start_ns;
  j["end_ns"] = r.end_ns;
  j["restore_time_s"] = r.restore_time_s;
  PutOptional(j, "available_transfer_s", r.available_transfer_s);
  j["path_allocations"] = Json::array();
  for (const auto &p : r.path_allocations)
    j["path_allocations"].push_back(Encode(p));
  return j;
}

template <typename T> Result<T> ParseRecord(std::string_view text) {
  try {
    auto j = Object(text);
    T r;
    r.raw_json = std::string(text);
    Decode(j, r);
    return r;
  } catch (const std::exception &e) {
    return Error{std::string("invalid trace v3 record: ") + e.what()};
  }
}
template <typename T> Result<std::string> SerializeRecord(const T &r) {
  try {
    auto text = Encode(r).dump();
    auto checked = ParseRecord<T>(text);
    if (!checked)
      return checked.error();
    return text;
  } catch (const std::exception &e) {
    return Error{std::string("cannot serialize trace v3 record: ") + e.what()};
  }
}
}

#define TRACE_IMPL(T)                                                          \
  template <> Result<T> Parse<T>(std::string_view json) {                      \
    return ParseRecord<T>(json);                                               \
  }                                                                            \
  template <> Result<std::string> Serialize<T>(const T &record) {              \
    return SerializeRecord(record);                                            \
  }

Result<std::vector<Node>> ParseNodes(std::string_view text) {
  try {
    auto j = Json::parse(text);
    Require(j.is_array(), "nodes.json must be an array");
    std::vector<Node> nodes;
    for (const auto &value : j) {
      auto node = ParseRecord<Node>(value.dump());
      if (!node)
        return node.error();
      nodes.push_back(std::move(node.value()));
    }
    return nodes;
  } catch (const std::exception &e) {
    return Error{std::string("invalid nodes trace: ") + e.what()};
  }
}
Result<std::string> SerializeNodes(const std::vector<Node> &nodes) {
  try {
    auto j = Json::array();
    for (const auto &node : nodes) {
      auto encoded = SerializeRecord(node);
      if (!encoded)
        return encoded.error();
      j.push_back(Json::parse(encoded.value()));
    }
    return j.dump();
  } catch (const std::exception &e) {
    return Error{std::string("cannot serialize nodes trace: ") + e.what()};
  }
}

#else
#define TRACE_IMPL(T)                                                          \
  template <> Result<T> Parse<T>(std::string_view) {                           \
    return Error{"trace v3 requires nlohmann_json (RECLAIMNET_HAVE_JSON)"};    \
  }                                                                            \
  template <> Result<std::string> Serialize<T>(const T &) {                    \
    return Error{"trace v3 requires nlohmann_json (RECLAIMNET_HAVE_JSON)"};    \
  }
Result<std::vector<Node>> ParseNodes(std::string_view) {
  return Error{"trace v3 requires nlohmann_json (RECLAIMNET_HAVE_JSON)"};
}
Result<std::string> SerializeNodes(const std::vector<Node> &) {
  return Error{"trace v3 requires nlohmann_json (RECLAIMNET_HAVE_JSON)"};
}
#endif

TRACE_IMPL(Schema)
TRACE_IMPL(Manifest)
TRACE_IMPL(BandwidthSample)
TRACE_IMPL(HazardSample)
TRACE_IMPL(ReclaimEvent)
TRACE_IMPL(P1Epoch)
TRACE_IMPL(Node)
TRACE_IMPL(Telemetry)
TRACE_IMPL(Job)
TRACE_IMPL(Flow)
TRACE_IMPL(Migration)
#undef TRACE_IMPL

}
