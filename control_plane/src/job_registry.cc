


#include "reclaimnet/job_registry.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

namespace reclaimnet {
namespace control {

VoidResult JobRegistry::add(const JobRecord& rec) {
  std::lock_guard<std::mutex> lk(mu_);
  if (jobs_.count(rec.job_id))
    return err_void("Job already registered: " + rec.job_id);
  jobs_.emplace(rec.job_id, rec);
  LOG_INFO("JobRegistry: registered job %s on node %s",
           rec.job_id.c_str(), rec.node_id.c_str());
  return ok_void();
}

VoidResult JobRegistry::update(const JobId& id,
                                std::function<void(JobRecord&)> mutator) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = jobs_.find(id);
  if (it == jobs_.end())
    return err_void("Job not found: " + id);
  mutator(it->second);
  it->second.last_update_ns = now_ns();
  return ok_void();
}

std::optional<JobRecord> JobRegistry::get(const JobId& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = jobs_.find(id);
  if (it == jobs_.end()) return std::nullopt;
  return it->second;
}

void JobRegistry::remove(const JobId& id) {
  std::lock_guard<std::mutex> lk(mu_);
  jobs_.erase(id);
  LOG_INFO("JobRegistry: removed job %s", id.c_str());
}

std::vector<JobRecord> JobRegistry::by_state(JobState s) const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<JobRecord> out;
  for (auto& [_, j] : jobs_)
    if (j.state == s) out.push_back(j);
  return out;
}

std::vector<JobRecord> JobRegistry::by_node(const NodeId& n) const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<JobRecord> out;
  for (auto& [_, j] : jobs_)
    if (j.node_id == n) out.push_back(j);
  return out;
}

size_t JobRegistry::size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return jobs_.size();
}

std::vector<JobRecord> JobRegistry::all() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<JobRecord> out;
  out.reserve(jobs_.size());
  for (auto& [_, j] : jobs_) out.push_back(j);
  return out;
}

}
}
