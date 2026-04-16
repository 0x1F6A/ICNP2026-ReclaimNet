#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/telemetry.h"

#include <string>
#include <memory>


namespace grpc { class Channel; }

namespace reclaimnet {
namespace agent {


class CoordinatorClient {
public:
  CoordinatorClient(std::string coordinator_addr,
                    bool mtls_enabled,
                    const std::string& cert_path,
                    const std::string& key_path,
                    const std::string& ca_path);


  VoidResult register_node(const std::string& node_id,
                            const std::string& address,
                            const std::string& building,
                            const std::string& switch_id,
                            uint64_t total_vram_bytes,
                            uint64_t link_capacity_bps);


  VoidResult register_job(const std::string& job_id,
                           const std::string& node_id,
                           uint64_t vram_bytes,
                           uint64_t checkpoint_size_bytes,
                           const std::string& user,
                           const std::string& tag);


  VoidResult heartbeat(const std::string& node_id,
                        const LocalTelemetry& tel);


  Result<ReclaimResponse> reclaim_signal(const ReclaimNotification& notif,
                                          const std::string& node_id);


  VoidResult migration_progress(const JobId& job_id,
                                 uint64_t bytes_transferred,
                                 bool completed);

  bool connected() const noexcept { return connected_; }

private:
  std::string addr_;
  bool        connected_ = false;


  std::shared_ptr<grpc::Channel> channel_;

  void ensure_connected();
};

}
}
