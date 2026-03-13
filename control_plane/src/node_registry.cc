/// @file node_registry.cc
/// @brief Thread-safe in-memory node registry.

#include "reclaimnet/node_registry.h"
#include "reclaimnet/log.h"

namespace reclaimnet {
namespace control {

void NodeRegistry::upsert(const NodeRecord& rec) {
  std::lock_guard<std::mutex> lk(mu_);
  auto [it, inserted] = nodes_.emplace(rec.node_id, rec);
  if (!inserted) it->second = rec;
  LOG_DEBUG("NodeRegistry: upserted node %s state=%d", rec.node_id.c_str(), (int)rec.state);
}

std::optional<NodeRecord> NodeRegistry::get(const NodeId& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = nodes_.find(id);
  if (it == nodes_.end()) return std::nullopt;
  return it->second;
}

VoidResult NodeRegistry::update_telemetry(const NodeId& id, const NodeTelemetry& tel) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = nodes_.find(id);
  if (it == nodes_.end()) return err_void("Unknown node: " + id);
  it->second.telemetry = tel;
  return ok_void();
}

VoidResult NodeRegistry::heartbeat(const NodeId& id, int64_t now_ns_val) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = nodes_.find(id);
  if (it == nodes_.end()) return err_void("Unknown node: " + id);
  it->second.last_heartbeat_ns = now_ns_val;
  it->second.state             = NodeState::ACTIVE;
  return ok_void();
}

std::vector<NodeId> NodeRegistry::mark_stale(int64_t threshold_ns) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<NodeId> changed;
  for (auto& [id, rec] : nodes_) {
    if (rec.state == NodeState::ACTIVE && rec.last_heartbeat_ns < threshold_ns) {
      rec.state = NodeState::UNREACHABLE;
      changed.push_back(id);
      LOG_WARN("NodeRegistry: marking node %s UNREACHABLE (no heartbeat)", id.c_str());
    }
  }
  return changed;
}

std::vector<NodeRecord> NodeRegistry::by_state(NodeState s) const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<NodeRecord> out;
  for (auto& [_, r] : nodes_)
    if (r.state == s) out.push_back(r);
  return out;
}

std::vector<NodeRecord> NodeRegistry::all() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<NodeRecord> out;
  out.reserve(nodes_.size());
  for (auto& [_, r] : nodes_) out.push_back(r);
  return out;
}

size_t NodeRegistry::size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return nodes_.size();
}

uint64_t NodeRegistry::free_vram(const NodeId& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = nodes_.find(id);
  if (it == nodes_.end()) return 0;
  return it->second.telemetry.vram_free_bytes;
}

}  // namespace control
}  // namespace reclaimnet
