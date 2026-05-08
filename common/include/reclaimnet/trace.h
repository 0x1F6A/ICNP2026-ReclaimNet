#pragma once

#include "reclaimnet/result.h"
#include "reclaimnet/types.h"

#include <cstddef>
#include <cstdint>
#include <istream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace reclaimnet::trace {

struct Record {
  std::string raw_json = "{}";
};

struct Schema : Record {
  int schema_version = 3;
};

struct Manifest : Record {
  int schema_version = 3;
  std::string run_id;
  std::string timezone;
  double p1_beta = 0.2;
  double p1_loss_budget_s = 18000.0;
  double p1_global_budget_bps = 0.0;
  double p3_beta = 0.3;
  double p3_bmin_bps = 0.0;
  double p3_min_useful_transfer_s = 1.0;
};

struct BandwidthSample : Record {
  NodeId node_id;
  uint64_t bps = 0;
  int64_t timestamp_ns = 0;
};

struct HazardSample : Record {
  NodeId node_id;
  double lambda_e = 0.0;
  int64_t timestamp_ns = 0;
};

struct ReclaimEvent : Record {
  NodeId node_id;
  ReclaimType type = ReclaimType::SCHEDULED;
  double tau_seconds = 0.0;
  int64_t timestamp_ns = 0;
};

struct P1Member : Record {
  JobId job_id;
  std::vector<NodeId> source_node_ids;
  std::vector<NodeId> replica_node_ids;
  uint64_t checkpoint_size_bytes = 0;
  double lambda_e = 0.0;
  double allocation_weight = 0.0;
  double write_cap_bps = 0.0;
  double b_i_bps = 0.0;
  double write_rate_limit_bps = 0.0;
  double p1_raw_interval_s = 0.0;
  double p1_lower_interval_s = 0.0;
  double p1_upper_interval_s = 0.0;
  double target_interval_s = 0.0;
  bool constraints_feasible = false;
};

struct P1Epoch : Record {
  std::string run_id;
  std::string allocation_epoch_id;
  int64_t start_ns = 0;
  int64_t end_ns = 0;
  double control_epoch_s = 30.0;
  double allocation_budget_bps = 0.0;
  double controller_ceiling_bps = 0.0;
  double beta = 0.2;
  double total_allocated_bps = 0.0;
  int water_fill_iterations = 0;
  std::vector<P1Member> members;
};

struct Node : Record {
  NodeId node_id;
  NodeState state = NodeState::UNKNOWN;
  std::string address;
  std::string building;
  std::string switch_id;
  uint64_t total_vram_bytes = 0;
  uint64_t link_capacity_bps = 0;
  int64_t registered_at_ns = 0;
  int64_t last_heartbeat_ns = 0;
};

struct Telemetry : Record {
  NodeId node_id;
  double gpu_util_pct = 0.0;
  double vram_used_pct = 0.0;
  uint64_t vram_free_bytes = 0;
  uint64_t link_tx_bps = 0;
  uint64_t link_rx_bps = 0;
  double cpu_load_1m = 0.0;
  int64_t timestamp_ns = 0;
};

struct Job : Record {
  JobId job_id;
  JobState state = JobState::UNKNOWN;
  std::vector<NodeId> node_ids;
  uint64_t logical_checkpoint_size_bytes = 0;
  uint64_t per_rank_checkpoint_size_bytes = 0;
  uint64_t logical_vram_bytes = 0;
  double lambda_i = 0.0;
  double loss_budget_s = 18000.0;
  int64_t submit_time_ns = 0;
  int64_t last_update_ns = 0;
};

using Edge = std::pair<NodeId, NodeId>;

struct Flow : Record {
  std::string flow_id;
  std::string flow_class;
  JobId job_id;
  NodeId src_node_id;
  NodeId dst_node_id;
  FiveTuple tuple;
  uint64_t bytes = 0;
  std::optional<uint64_t> planned_bytes;
  int64_t ts_ns = 0;
  int64_t end_ns = 0;
  std::optional<double> average_rate_bps;
  std::optional<double> rate_bps;
  std::optional<double> required_bps;
  TopologyLevel topology_level = TopologyLevel::INTRA_RACK;
  std::vector<Edge> path_edges;
};

struct PathAllocation : Record {
  int rank = 0;
  NodeId src_node_id;
  NodeId dst_node_id;
  uint64_t bytes = 0;
  double required_bps = 0.0;
  double allocated_bps = 0.0;
  std::vector<Edge> path_edges;
};

struct Migration : Record {
  std::string migration_id;
  JobId job_id;
  NodeId src_node_id;
  std::optional<NodeId> dst_node_id;
  ReclaimType reclaim_type = ReclaimType::SCHEDULED;
  MigrationPhase phase = MigrationPhase::IDLE;
  TopologyLevel topology_level = TopologyLevel::INTRA_RACK;
  bool p3_applicable = false;
  std::string p3_status;
  uint64_t bytes_to_transfer_total = 0;
  double deadline_s = 0.0;
  std::optional<int64_t> deadline_ns;
  int64_t decision_ns = 0;
  int64_t start_ns = 0;
  int64_t end_ns = 0;
  double restore_time_s = 0.0;
  std::optional<double> available_transfer_s;
  std::vector<PathAllocation> path_allocations;
};

template <typename T> Result<T> Parse(std::string_view json);
template <typename T> Result<std::string> Serialize(const T &record);

#define RECLAIMNET_TRACE_TYPE(T)                                               \
  template <> Result<T> Parse<T>(std::string_view json);                       \
  template <> Result<std::string> Serialize<T>(const T &record)
RECLAIMNET_TRACE_TYPE(Schema);
RECLAIMNET_TRACE_TYPE(Manifest);
RECLAIMNET_TRACE_TYPE(BandwidthSample);
RECLAIMNET_TRACE_TYPE(HazardSample);
RECLAIMNET_TRACE_TYPE(ReclaimEvent);
RECLAIMNET_TRACE_TYPE(P1Epoch);
RECLAIMNET_TRACE_TYPE(Node);
RECLAIMNET_TRACE_TYPE(Telemetry);
RECLAIMNET_TRACE_TYPE(Job);
RECLAIMNET_TRACE_TYPE(Flow);
RECLAIMNET_TRACE_TYPE(Migration);
#undef RECLAIMNET_TRACE_TYPE

Result<std::vector<Node>> ParseNodes(std::string_view json);
Result<std::string> SerializeNodes(const std::vector<Node> &nodes);
Result<std::vector<NodeId>> PathNodes(const NodeId &src, const NodeId &dst,
                                      const std::vector<Edge> &edges);

template <typename T, typename Consumer>
Result<size_t> ReadJsonLines(std::istream &stream, Consumer consume) {
  size_t line_no = 0;
  size_t count = 0;
  std::string line;
  while (std::getline(stream, line)) {
    ++line_no;
    if (line.find_first_not_of(" \t\r") == std::string::npos)
      continue;
    auto parsed = Parse<T>(line);
    if (!parsed)
      return Error{"trace line " + std::to_string(line_no) + ": " +
                   parsed.error().message};
    auto accepted = consume(parsed.value());
    if (!accepted)
      return Error{"trace line " + std::to_string(line_no) + ": " +
                   accepted.error().message};
    ++count;
  }
  if (stream.bad())
    return Error{"trace input read failed"};
  return count;
}

template <typename Input>
std::vector<Input> ToP1Inputs(const P1Epoch &epoch, double loss_budget_s) {
  std::vector<Input> result;
  result.reserve(epoch.members.size());
  for (const auto &member : epoch.members) {
    Input input{};
    input.job_id = member.job_id;
    input.lambda_i = member.lambda_e;
    input.checkpoint_size_b = member.checkpoint_size_bytes;
    input.write_cap_bps = member.write_cap_bps;
    input.checkpoint_beta = epoch.beta;
    input.loss_budget_s = loss_budget_s;
    result.push_back(std::move(input));
  }
  return result;
}

template <typename Entry>
Result<Entry> ToP3Entry(const Migration &migration, size_t path_index = 0) {
  if (!migration.p3_applicable ||
      migration.reclaim_type != ReclaimType::SCHEDULED)
    return Error{"P3 is not applicable to this migration"};
  if (!migration.deadline_ns || path_index >= migration.path_allocations.size())
    return Error{"P3 migration requires an absolute deadline and a rank path"};
  const auto &path = migration.path_allocations[path_index];
  auto nodes = PathNodes(path.src_node_id, path.dst_node_id, path.path_edges);
  if (!nodes)
    return nodes.error();
  Entry entry{};
  entry.job_id = migration.job_id;
  if (migration.path_allocations.size() > 1)
    entry.job_id += "#rank-" + std::to_string(path.rank);
  entry.src = path.src_node_id;
  entry.dst = path.dst_node_id;
  entry.data_remaining_bytes = path.bytes;
  entry.data_total_bytes = path.bytes;
  entry.bytes_done = 0;
  entry.deadline_s = migration.deadline_s;
  entry.deadline_ns = *migration.deadline_ns;
  entry.start_ns = migration.decision_ns;

  if (migration.available_transfer_s) {
    if (*migration.available_transfer_s <= 0 ||
        *migration.available_transfer_s > migration.deadline_s)
      return Error{"P3 available transfer time must be within the notice"};
    entry.restore_time_s =
        migration.deadline_s - *migration.available_transfer_s;
  } else {
    entry.restore_time_s = migration.restore_time_s;
  }
  entry.allocated_bps = 0.0;
  entry.emergency = false;
  entry.path_nodes = std::move(nodes.value());
  return entry;
}

}
