#pragma once



#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/config.h"
#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/migration_client.h"
#include "reclaimnet/ebpf_loader.h"
#include "reclaimnet/flow_table.h"
#include "reclaimnet/telemetry.h"
#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/grpc_client.h"

#include <atomic>
#include <thread>
#include <memory>

namespace reclaimnet {
namespace agent {


class NodeAgent {
public:
  explicit NodeAgent(AgentConfig cfg);
  ~NodeAgent();


  VoidResult start();


  void stop();


  VoidResult handle_reclaim(const ReclaimNotification& notif);

  bool running() const noexcept { return running_.load(); }

private:
  AgentConfig cfg_;
  std::atomic<bool> running_{false};


  std::unique_ptr<CheckpointManager>  ckpt_mgr_;
  std::unique_ptr<MigrationClient>    mig_client_;
  std::unique_ptr<TelemetryCollector> telemetry_;
  std::unique_ptr<CoordinatorClient>  grpc_client_;
  std::unique_ptr<ReclaimHandler>     reclaim_handler_;
  FlowTable                           flow_table_;
  EbpfHandle                          ebpf_tc_;


  std::thread heartbeat_thread_;
  void        run_heartbeat_loop();
};

}
}
