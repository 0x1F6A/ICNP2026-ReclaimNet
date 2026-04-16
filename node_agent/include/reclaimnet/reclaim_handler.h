#pragma once




#include "reclaimnet/types.h"
#include "reclaimnet/result.h"
#include "reclaimnet/checkpoint_manager.h"
#include "reclaimnet/migration_client.h"

#include <functional>
#include <string>

namespace reclaimnet {
namespace agent {


struct ReclaimNotification {
  JobId        job_id;
  double       deadline_s;
  ReclaimType  type;
  uint64_t     vram_bytes;
};


struct ReclaimResponse {
  NodeId   dst_node;
  uint16_t dst_port;
  double   allocated_bps;
  double   deadline_s;
};


using ReclaimSignalCb = std::function<Result<ReclaimResponse>(const ReclaimNotification&)>;


class ReclaimHandler {
public:
  ReclaimHandler(CheckpointManager& ckpt_mgr,
                 MigrationClient&   mig_client,
                 ReclaimSignalCb    signal_cb);


  VoidResult handle(const ReclaimNotification& notif);

private:
  CheckpointManager& ckpt_mgr_;
  MigrationClient&   mig_client_;
  ReclaimSignalCb    signal_cb_;
};

}
}
