#include "reclaimnet/p3_admission.h"
#include "reclaimnet/time.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <unordered_set>

namespace reclaimnet {
namespace control {
namespace {
std::string group_key(const MigrationEntry &entry) {
  const auto &id =
      entry.migration_id.empty() ? entry.job_id : entry.migration_id;
  return std::to_string(id.size()) + ":" + id;
}
double rate_cap(const MigrationEntry &entry) {
  if (entry.data_remaining_bytes == 0)
    return 0;
  double cap = entry.flow_cap_bps > 0 ? entry.flow_cap_bps
                                      : std::numeric_limits<double>::infinity();
  if (entry.minimum_useful_transfer_s > 0)
    cap = std::min(cap, bytes_to_bits(entry.data_total_bytes) /
                            entry.minimum_useful_transfer_s);
  return cap;
}
bool periodic(const MigrationEntry &entry) {
  return entry.traffic_class == MigrationTrafficClass::PERIODIC_CHECKPOINT;
}
}

std::string P3Admission::link_key(const NodeId &src, const NodeId &dst) {
  const auto &first = src < dst ? src : dst;
  const auto &second = src < dst ? dst : src;
  return std::to_string(first.size()) + ":" + first + second;
}
std::string P3Admission::migration_key(const MigrationEntry &entry) {
  return group_key(entry) + ":" + std::to_string(entry.rank);
}
double P3Admission::available_bps(const std::string &key) const {
  auto found = links_.find(key);
  if (found == links_.end())
    return 0;
  double reserve = std::max(research_min_bps_, (1.0 - migration_share_) *
                                                   found->second.capacity_bps);
  return std::max(0.0, found->second.capacity_bps - reserve -
                           found->second.allocated_bps);
}
P3Admission::P3Admission(double share, double floor)
    : migration_share_(share), research_min_bps_(floor) {
  if (!std::isfinite(share) || share < 0 || share > 1 ||
      !std::isfinite(floor) || floor < 0)
    throw std::invalid_argument("Invalid migration share or research reserve");
}
void P3Admission::add_link(const NodeId &src, const NodeId &dst,
                           double capacity) {
  if (src.empty() || dst.empty() || src == dst || !std::isfinite(capacity) ||
      capacity < 0)
    throw std::invalid_argument("Invalid migration link");
  std::lock_guard<std::mutex> lock(mu_);
  auto [found, inserted] =
      links_.emplace(link_key(src, dst), LinkState{src, dst, capacity, 0, 0});
  if (!inserted)
    found->second.capacity_bps = capacity;
}
void P3Admission::set_destination_resources(const NodeId &dst, uint64_t vram,
                                            uint32_t gpu) {
  if (dst.empty())
    throw std::invalid_argument("Empty destination resource identifier");
  std::lock_guard<std::mutex> lock(mu_);
  auto &state = destinations_[dst];
  state.free_vram_bytes = vram;
  state.free_gpu_count = gpu;
}
bool P3Admission::same_request(const MigrationEntry &first,
                               const MigrationEntry &second) const {
  const auto path = first.path_nodes.empty()
                        ? std::vector<NodeId>{first.src, first.dst}
                        : first.path_nodes;
  const uint64_t total =
      first.data_total_bytes > 0 ? first.data_total_bytes
      : first.data_remaining_bytes <=
              std::numeric_limits<uint64_t>::max() - first.bytes_done
          ? first.data_remaining_bytes + first.bytes_done
          : 0;
  return first.job_id == second.job_id && first.src == second.src &&
         first.dst == second.dst && first.migration_id == second.migration_id &&
         first.rank == second.rank && first.gang_size == second.gang_size &&
         path == second.path_nodes && total == second.data_total_bytes &&
         first.deadline_s == second.deadline_s &&
         first.restore_time_s == second.restore_time_s &&
         (first.start_ns == 0 || first.start_ns == second.start_ns) &&
         (first.deadline_ns == 0 || first.deadline_ns == second.deadline_ns) &&
         first.flow_cap_bps == second.flow_cap_bps &&
         first.minimum_useful_transfer_s == second.minimum_useful_transfer_s &&
         first.vram_required_bytes == second.vram_required_bytes &&
         first.gpu_count_required == second.gpu_count_required &&
         first.traffic_class == second.traffic_class;
}
Result<MigrationEntry> P3Admission::normalize(const MigrationEntry &request,
                                              int64_t decision_ns,
                                              bool strict_now) const {
  if (request.job_id.empty() || request.src.empty() || request.dst.empty() ||
      request.src == request.dst || !std::isfinite(request.deadline_s) ||
      request.deadline_s <= 0 || !std::isfinite(request.restore_time_s) ||
      request.restore_time_s < 0 || !std::isfinite(request.flow_cap_bps) ||
      request.flow_cap_bps < 0 ||
      !std::isfinite(request.minimum_useful_transfer_s) ||
      request.minimum_useful_transfer_s < 0 || request.start_ns < 0 ||
      request.eligible_start_ns < 0 || request.gang_size == 0 ||
      request.rank >= request.gang_size)
    return Error{"Invalid migration request"};
  MigrationEntry entry = request;
  if (entry.start_ns == 0)
    entry.start_ns = decision_ns > 0 ? decision_ns : now_ns();
  if (entry.path_nodes.empty())
    entry.path_nodes = {entry.src, entry.dst};
  if (entry.path_nodes.size() < 2 || entry.path_nodes.front() != entry.src ||
      entry.path_nodes.back() != entry.dst)
    return Error{"Path endpoints do not match"};
  std::unordered_set<NodeId> visited;
  for (const auto &node : entry.path_nodes)
    if (node.empty() || !visited.insert(node).second)
      return Error{"Invalid or cyclic migration path"};
  if (entry.data_total_bytes == 0) {
    if (entry.data_remaining_bytes >
        std::numeric_limits<uint64_t>::max() - entry.bytes_done)
      return Error{"Migration byte count overflow"};
    entry.data_total_bytes = entry.data_remaining_bytes + entry.bytes_done;
  }
  if (entry.bytes_done > entry.data_total_bytes)
    return Error{"Invalid migration progress"};
  entry.data_remaining_bytes = entry.data_total_bytes - entry.bytes_done;
  const long double duration_ns =
      std::round(static_cast<long double>(entry.deadline_s) * 1e9L);
  if (duration_ns < 1 ||
      duration_ns > static_cast<long double>(
                        std::numeric_limits<int64_t>::max() - entry.start_ns))
    return Error{"Migration deadline overflow"};
  const int64_t deadline = entry.start_ns + static_cast<int64_t>(duration_ns);
  if (entry.deadline_ns != 0 && entry.deadline_ns != deadline)
    return Error{"Absolute deadline disagrees with notice"};
  entry.deadline_ns = deadline;
  if (entry.eligible_start_ns == 0)
    entry.eligible_start_ns = entry.start_ns;
  if (entry.data_remaining_bytes == 0) {
    if (strict_now && deadline <= decision_ns)
      return Error{"Migration deadline has expired"};
    entry.minimum_required_bps = 0;
    entry.allocated_bps = 0;
    return entry;
  }
  const int64_t transfer_start = std::max(
      entry.eligible_start_ns, strict_now ? decision_ns : entry.start_ns);
  const long double transfer_s =
      static_cast<long double>(entry.deadline_ns - transfer_start) / 1e9L -
      entry.restore_time_s;
  if (transfer_s <= 0)
    return Error{"Migration deadline has expired"};
  const long double required = 8.0L * entry.data_remaining_bytes / transfer_s;
  if (!std::isfinite(required) || required > std::numeric_limits<double>::max())
    return Error{"Required migration rate is not finite"};
  entry.minimum_required_bps = static_cast<double>(required);
  const double cap = rate_cap(entry);
  if (entry.minimum_required_bps > cap + 1e-12 * std::max(1.0, cap))
    return Error{"Deadline requires a rate above the flow cap"};
  entry.minimum_required_bps = std::min(entry.minimum_required_bps, cap);
  entry.allocated_bps = entry.minimum_required_bps;
  return entry;
}
VoidResult P3Admission::reserve(const MigrationEntry &entry) {
  for (size_t i = 1; i < entry.path_nodes.size(); ++i) {
    const auto key = link_key(entry.path_nodes[i - 1], entry.path_nodes[i]);
    if (links_.find(key) == links_.end())
      return err_void("Unknown edge on migration path");
    if (entry.allocated_bps > available_bps(key) + 1e-8)
      return err_void("Insufficient deadline bandwidth on migration path");
  }
  if (entry.vram_required_bytes > 0 || entry.gpu_count_required > 0) {
    const auto destination = destinations_.find(entry.dst);
    if (destination == destinations_.end())
      return err_void("Destination resource capacity is unknown");
    const auto &resource = destination->second;
    if (resource.allocated_vram_bytes > resource.free_vram_bytes ||
        entry.vram_required_bytes >
            resource.free_vram_bytes - resource.allocated_vram_bytes ||
        resource.allocated_gpu_count > resource.free_gpu_count ||
        entry.gpu_count_required >
            resource.free_gpu_count - resource.allocated_gpu_count)
      return err_void("Insufficient destination GPU or VRAM reservation");
  }
  for (size_t i = 1; i < entry.path_nodes.size(); ++i) {
    auto &link =
        links_.at(link_key(entry.path_nodes[i - 1], entry.path_nodes[i]));
    link.allocated_bps += entry.allocated_bps;
    ++link.active_count;
  }
  if (entry.vram_required_bytes > 0 || entry.gpu_count_required > 0) {
    auto &resource = destinations_.at(entry.dst);
    resource.allocated_vram_bytes += entry.vram_required_bytes;
    resource.allocated_gpu_count += entry.gpu_count_required;
  }
  return ok_void();
}
void P3Admission::unreserve(const MigrationEntry &entry) {
  for (size_t i = 1; i < entry.path_nodes.size(); ++i) {
    auto &link =
        links_.at(link_key(entry.path_nodes[i - 1], entry.path_nodes[i]));
    link.allocated_bps =
        std::max(0.0, link.allocated_bps - entry.allocated_bps);
    if (link.active_count > 0)
      --link.active_count;
    if (link.active_count == 0)
      link.allocated_bps = 0;
  }
  if (entry.vram_required_bytes > 0 || entry.gpu_count_required > 0) {
    auto &resource = destinations_.at(entry.dst);
    resource.allocated_vram_bytes -=
        std::min(resource.allocated_vram_bytes, entry.vram_required_bytes);
    resource.allocated_gpu_count -=
        std::min(resource.allocated_gpu_count, entry.gpu_count_required);
  }
}
Result<MigrationEntry> P3Admission::admit(const MigrationEntry &request) {
  std::lock_guard<std::mutex> lock(mu_);
  auto existing = migrations_.find(migration_key(request));
  if (existing != migrations_.end()) {
    if (same_request(request, existing->second))
      return existing->second;
    return Error{"Conflicting migration already active"};
  }
  if (request.gang_size != 1)
    return Error{"Gang migrations require batch admission"};
  auto normalized = normalize(
      request, request.start_ns > 0 ? request.start_ns : now_ns(), false);
  if (!normalized)
    return normalized.error();
  auto entry = normalized.value();
  if (auto reservation = reserve(entry); !reservation)
    return reservation.error();
  entry.allocation_version = next_version_++;
  migrations_.emplace(migration_key(entry), entry);
  return entry;
}
void P3Admission::rebuild_link_usage() {
  for (auto &[_, link] : links_) {
    link.allocated_bps = 0;
    link.active_count = 0;
  }
  for (auto &[_, resource] : destinations_) {
    resource.allocated_gpu_count = 0;
    resource.allocated_vram_bytes = 0;
  }
  for (const auto &[_, entry] : migrations_) {
    for (size_t i = 1; i < entry.path_nodes.size(); ++i) {
      auto &link =
          links_.at(link_key(entry.path_nodes[i - 1], entry.path_nodes[i]));
      link.allocated_bps += entry.allocated_bps;
      ++link.active_count;
    }
    if (entry.vram_required_bytes > 0 || entry.gpu_count_required > 0) {
      auto &resource = destinations_.at(entry.dst);
      resource.allocated_gpu_count += entry.gpu_count_required;
      resource.allocated_vram_bytes += entry.vram_required_bytes;
    }
  }
}
std::vector<MigrationEntry> P3Admission::rebalance(int64_t) {
  std::map<std::string, std::vector<std::string>> units;
  for (const auto &[key, entry] : migrations_)
    units[periodic(entry) ? key : group_key(entry)].push_back(key);
  auto unit_rate = [&](const std::string &unit) {
    double rate = 0;
    for (const auto &key : units.at(unit))
      rate = std::max(rate, migrations_.at(key).allocated_bps);
    return rate;
  };
  auto unit_cap = [&](const std::string &unit) {
    double cap = std::numeric_limits<double>::infinity();
    for (const auto &key : units.at(unit))
      if (migrations_.at(key).data_remaining_bytes > 0)
        cap = std::min(cap, rate_cap(migrations_.at(key)));
    return std::isfinite(cap) ? cap : 0.0;
  };
  for (const bool background : {false, true}) {
    std::set<std::string> free;
    for (const auto &[unit, keys] : units)
      if (periodic(migrations_.at(keys.front())) == background &&
          unit_rate(unit) < unit_cap(unit))
        free.insert(unit);
    while (!free.empty()) {
      for (auto found = free.begin(); found != free.end();) {
        bool limited = unit_cap(*found) - unit_rate(*found) <= 1e-7;
        for (const auto &key : units.at(*found)) {
          const auto &entry = migrations_.at(key);
          if (entry.data_remaining_bytes == 0)
            continue;
          for (size_t i = 1; i < entry.path_nodes.size(); ++i)
            if (available_bps(link_key(entry.path_nodes[i - 1],
                                       entry.path_nodes[i])) <= 1e-7)
              limited = true;
        }
        if (limited)
          found = free.erase(found);
        else
          ++found;
      }
      if (free.empty())
        break;
      double level = std::numeric_limits<double>::infinity();
      for (const auto &unit : free)
        level = std::min(level, unit_rate(unit));
      std::vector<std::string> filling;
      double increment = std::numeric_limits<double>::infinity();
      for (const auto &unit : free) {
        if (unit_rate(unit) <= level + 1e-7) {
          filling.push_back(unit);
          increment = std::min(increment, unit_cap(unit) - unit_rate(unit));
        } else
          increment = std::min(increment, unit_rate(unit) - level);
      }
      std::map<std::string, size_t> edge_demand;
      for (const auto &unit : filling)
        for (const auto &key : units.at(unit)) {
          const auto &entry = migrations_.at(key);
          if (entry.data_remaining_bytes == 0)
            continue;
          for (size_t i = 1; i < entry.path_nodes.size(); ++i)
            ++edge_demand[link_key(entry.path_nodes[i - 1],
                                   entry.path_nodes[i])];
        }
      for (const auto &[edge, count] : edge_demand)
        increment = std::min(increment, available_bps(edge) / count);
      if (!std::isfinite(increment) || increment <= 1e-10)
        break;
      for (const auto &unit : filling)
        for (const auto &key : units.at(unit))
          if (migrations_.at(key).data_remaining_bytes > 0)
            migrations_.at(key).allocated_bps += increment;
      rebuild_link_usage();
    }
  }
  std::vector<MigrationEntry> result;
  for (const auto &[_, entry] : migrations_)
    result.push_back(entry);
  return result;
}
BatchAdmissionResult
P3Admission::admit_batch(const std::vector<MigrationEntry> &requests,
                         int64_t decision_ns) {
  if (decision_ns <= 0)
    decision_ns = now_ns();
  std::lock_guard<std::mutex> lock(mu_);
  BatchAdmissionResult result;
  const auto previous = migrations_;
  auto pool = migrations_;
  for (const auto &request : requests) {
    const auto key = migration_key(request);
    const auto current = pool.find(key);
    if (current != pool.end()) {
      if (!same_request(request, current->second))
        result.degraded.push_back(
            {request, "Conflicting migration request", !periodic(request)});
    } else {
      auto normalized = normalize(request, decision_ns, true);
      if (!normalized)
        result.degraded.push_back(
            {request, normalized.error().message, !periodic(request)});
      else
        pool.emplace(key, normalized.value());
    }
  }
  std::map<std::string, std::vector<MigrationEntry>> groups;
  for (const auto &[_, entry] : pool)
    groups[group_key(entry)].push_back(entry);
  std::vector<std::vector<MigrationEntry>> ordered;
  for (auto &[_, group] : groups)
    ordered.push_back(std::move(group));
  std::sort(ordered.begin(), ordered.end(),
            [decision_ns](const auto &first, const auto &second) {
              if (periodic(first.front()) != periodic(second.front()))
                return !periodic(first.front());
              auto absolute = [decision_ns](const MigrationEntry &entry) {
                return entry.deadline_ns > 0
                           ? static_cast<long double>(entry.deadline_ns)
                           : static_cast<long double>(entry.start_ns > 0
                                                          ? entry.start_ns
                                                          : decision_ns) +
                                 entry.deadline_s * 1e9L;
              };
              const auto a = absolute(first.front());
              const auto b = absolute(second.front());
              return a != b
                         ? a < b
                         : group_key(first.front()) < group_key(second.front());
            });
  migrations_.clear();
  rebuild_link_usage();
  for (const auto &group : ordered) {
    std::string failure;
    std::vector<MigrationEntry> normalized;
    std::set<uint32_t> ranks;
    const auto &first = group.front();
    if (group.size() != first.gang_size)
      failure = "Incomplete gang migration";
    for (const auto &request : group) {
      if (request.job_id != first.job_id ||
          request.gang_size != first.gang_size ||
          !ranks.insert(request.rank).second)
        failure = "Inconsistent gang migration";
      auto entry = normalize(request, decision_ns, true);
      if (!entry)
        failure = entry.error().message;
      else
        normalized.push_back(entry.value());
    }
    if (failure.empty()) {
      double common_rate = 0;
      for (const auto &entry : normalized)
        common_rate = std::max(common_rate, entry.minimum_required_bps);
      for (auto &entry : normalized) {
        const double rate =
            periodic(entry) ? entry.minimum_required_bps : common_rate;
        if (entry.data_remaining_bytes > 0 && rate > rate_cap(entry))
          failure = "Gang deadline exceeds a rank flow cap";
        entry.allocated_bps = entry.data_remaining_bytes > 0 ? rate : 0;
      }
    }
    if (failure.empty()) {
      size_t reserved = 0;
      for (const auto &entry : normalized) {
        auto reservation = reserve(entry);
        if (!reservation) {
          failure = reservation.error().message;
          break;
        }
        ++reserved;
      }
      if (!failure.empty())
        for (size_t i = 0; i < reserved; ++i)
          unreserve(normalized[i]);
      else
        for (auto entry : normalized)
          migrations_.emplace(migration_key(entry), std::move(entry));
    }
    if (!failure.empty())
      for (const auto &entry : group)
        result.degraded.push_back({entry, failure, !periodic(entry)});
  }
  rebalance(decision_ns);
  for (auto &[key, entry] : migrations_) {
    const std::string accepted_key = key;
    const auto before = previous.find(key);
    if (before == previous.end() ||
        std::abs(before->second.allocated_bps - entry.allocated_bps) >
            1e-10 * std::max(1.0, entry.allocated_bps)) {
      entry.allocation_version = next_version_++;
      result.rate_updates.push_back(entry);
    } else
      entry.allocation_version = before->second.allocation_version;
    result.admitted.push_back(entry);
    degraded_.erase(std::remove_if(degraded_.begin(), degraded_.end(),
                                   [&](const auto &rejected) {
                                     return migration_key(rejected.entry) ==
                                            accepted_key;
                                   }),
                    degraded_.end());
  }
  for (const auto &[key, before] : previous) {
    if (migrations_.find(key) == migrations_.end()) {
      auto cancelled = before;
      cancelled.allocated_bps = 0;
      cancelled.allocation_version = next_version_++;
      result.rate_updates.push_back(std::move(cancelled));
    }
  }
  for (const auto &rejected : result.degraded) {
    const auto key = migration_key(rejected.entry);
    if (migrations_.find(key) != migrations_.end())
      continue;
    degraded_.erase(std::remove_if(degraded_.begin(), degraded_.end(),
                                   [&](const auto &old) {
                                     return migration_key(old.entry) == key;
                                   }),
                    degraded_.end());
    degraded_.push_back(rejected);
  }
  return result;
}
BatchAdmissionResult P3Admission::schedule_checkpoints(
    const std::vector<PeriodicCheckpointDemand> &demands, int64_t decision_ns) {
  if (decision_ns <= 0)
    decision_ns = now_ns();
  std::set<std::string> group_ids;
  for (const auto &demand : demands)
    group_ids.insert(demand.demand_id.empty() ? demand.job_id
                                              : demand.demand_id);
  std::map<std::string, size_t> positions;
  size_t position = 0;
  for (const auto &id : group_ids)
    positions[id] = position++;
  std::vector<MigrationEntry> requests;
  for (const auto &demand : demands) {
    MigrationEntry entry;
    entry.job_id = demand.job_id;
    entry.src = demand.src;
    entry.dst = demand.dst;
    entry.data_remaining_bytes = demand.checkpoint_size_bytes;
    entry.data_total_bytes = demand.checkpoint_size_bytes;
    entry.bytes_done =
        std::min(demand.bytes_done, demand.checkpoint_size_bytes);
    entry.data_remaining_bytes -= entry.bytes_done;
    entry.start_ns = decision_ns;
    const double interval =
        std::min(demand.interval_s, 2.0 * demand.loss_budget_s);
    entry.deadline_s = static_cast<double>(
        std::ceil(static_cast<long double>(interval) * 1e9L) / 1e9L);
    entry.flow_cap_bps = demand.write_rate_limit_bps;
    entry.path_nodes = demand.path_nodes;
    entry.migration_id =
        demand.demand_id.empty()
            ? demand.job_id + ":checkpoint:" + std::to_string(decision_ns)
            : demand.demand_id;
    entry.rank = demand.rank;
    entry.gang_size = demand.gang_size;
    entry.traffic_class = MigrationTrafficClass::PERIODIC_CHECKPOINT;
    entry.minimum_useful_transfer_s = 0.0;
    entry.restore_required = false;
    const auto group =
        demand.demand_id.empty() ? demand.job_id : demand.demand_id;
    const long double offset_ns = entry.deadline_s * 1e9L * positions[group] /
                                  std::max<size_t>(1, group_ids.size());
    if (std::isfinite(offset_ns) && offset_ns >= 0 &&
        offset_ns < std::numeric_limits<int64_t>::max() - decision_ns)
      entry.eligible_start_ns =
          decision_ns + static_cast<int64_t>(std::round(offset_ns));
    else
      entry.deadline_s = -1;
    if (!std::isfinite(demand.write_rate_limit_bps) ||
        demand.write_rate_limit_bps <= 0)
      entry.flow_cap_bps = -1;
    requests.push_back(std::move(entry));
  }
  return admit_batch(requests, decision_ns);
}
std::optional<MigrationEntry>
P3Admission::get_reservation(const JobId &job, uint32_t rank,
                             const std::string &id) const {
  std::lock_guard<std::mutex> lock(mu_);
  for (const auto &[_, entry] : migrations_)
    if (entry.job_id == job && entry.rank == rank &&
        (id.empty() || entry.migration_id == id))
      return entry;
  return std::nullopt;
}
void P3Admission::release(const JobId &job) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto found = migrations_.begin(); found != migrations_.end();) {
    if (found->second.job_id == job) {
      unreserve(found->second);
      found = migrations_.erase(found);
    } else
      ++found;
  }
}
void P3Admission::release_migration(const std::string &id) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto found = migrations_.begin(); found != migrations_.end();) {
    if (found->second.migration_id == id) {
      unreserve(found->second);
      found = migrations_.erase(found);
    } else
      ++found;
  }
}
void P3Admission::update_progress(const JobId &job, uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto &[_, entry] : migrations_)
    if (entry.job_id == job && entry.rank == 0) {
      entry.bytes_done =
          std::max(entry.bytes_done, std::min(bytes, entry.data_total_bytes));
      entry.data_remaining_bytes = entry.data_total_bytes - entry.bytes_done;
    }
}
void P3Admission::update_rank_progress(const std::string &id, uint32_t rank,
                                       uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto &[_, entry] : migrations_)
    if (entry.migration_id == id && entry.rank == rank) {
      entry.bytes_done =
          std::max(entry.bytes_done, std::min(bytes, entry.data_total_bytes));
      entry.data_remaining_bytes = entry.data_total_bytes - entry.bytes_done;
    }
}
VoidResult P3Admission::update_payload(const std::string &id, uint32_t rank,
                                       uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto &[_, entry] : migrations_)
    if (entry.migration_id == id && entry.rank == rank) {
      entry.data_total_bytes = bytes;
      entry.bytes_done = std::min(entry.bytes_done, bytes);
      entry.data_remaining_bytes = bytes - entry.bytes_done;
      return ok_void();
    }
  return err_void("Unknown migration rank payload");
}
std::vector<MigrationEntry>
P3Admission::expired_migrations(int64_t time) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<MigrationEntry> result;
  for (const auto &[_, entry] : migrations_)
    if (entry.deadline_ns <= time)
      result.push_back(entry);
  return result;
}
std::vector<MigrationEntry> P3Admission::active_migrations() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<MigrationEntry> result;
  for (const auto &[_, entry] : migrations_)
    result.push_back(entry);
  return result;
}
std::vector<LinkState> P3Admission::link_states() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<LinkState> result;
  for (const auto &[_, entry] : links_)
    result.push_back(entry);
  return result;
}
std::vector<DegradedMigration> P3Admission::degraded_migrations() const {
  std::lock_guard<std::mutex> lock(mu_);
  return degraded_;
}
double P3Admission::residual_bps(const NodeId &src, const NodeId &dst) const {
  std::lock_guard<std::mutex> lock(mu_);
  return available_bps(link_key(src, dst));
}
void P3Admission::emergency_drain(const NodeId &src) {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto &[_, entry] : migrations_)
    if (entry.src == src)
      entry.emergency = true;
}
size_t P3Admission::active_count() const {
  std::lock_guard<std::mutex> lock(mu_);
  return migrations_.size();
}

}
}
