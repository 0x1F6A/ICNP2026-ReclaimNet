#pragma once







#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/topology.h"

#include <unordered_map>
#include <vector>
#include <mutex>
#include <string>

namespace reclaimnet {
namespace control {


struct MigrationEntry {
  JobId    job_id;
  NodeId   src;
  NodeId   dst;
  uint64_t data_remaining_bytes;
  double   deadline_s;
  double   allocated_bps;
  int64_t  start_ns;
  bool     emergency;
};


struct LinkState {
  NodeId   src;
  NodeId   dst;
  double   capacity_bps;
  double   allocated_bps;
  size_t   active_count;
};


class P3Admission {
public:

  explicit P3Admission(double migration_share = 0.40);


  void add_link(const NodeId& src, const NodeId& dst, double cap_bps);



  VoidResult admit(const MigrationEntry& entry);


  void release(const JobId& job_id);


  void update_progress(const JobId& job_id, uint64_t bytes_done);


  std::vector<MigrationEntry> expired_migrations(double now_s) const;


  std::vector<MigrationEntry> active_migrations() const;


  std::vector<LinkState> link_states() const;


  void emergency_drain(const NodeId& src);

  size_t active_count() const;

private:
  double migration_share_;
  mutable std::mutex mu_;

  std::unordered_map<JobId, MigrationEntry> migrations_;

  std::unordered_map<std::string, LinkState> links_;

  static std::string link_key(const NodeId& s, const NodeId& d);
  double available_bps(const std::string& key) const;
};

}
}
