


#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <atomic>

namespace reclaimnet {
namespace agent {

ReclaimHandler::ReclaimHandler(CheckpointManager& ckpt_mgr,
                                MigrationClient&   mig_client,
                                ReclaimSignalCb    signal_cb)
  : ckpt_mgr_(ckpt_mgr),
    mig_client_(mig_client),
    signal_cb_(std::move(signal_cb)) {}

VoidResult ReclaimHandler::handle(const ReclaimNotification& notif) {
  LOG_INFO("ReclaimHandler: handling job=%s type=%d deadline=%.1fs",
           notif.job_id.c_str(), (int)notif.type, notif.deadline_s);


  CheckpointCallbacks cb;
  cb.on_progress = [](const JobId& jid, uint64_t b) {
    LOG_DEBUG("Checkpoint progress: job=%s bytes=%llu", jid.c_str(), (unsigned long long)b);
  };
  cb.on_complete = [](const JobId& jid, bool ok) {
    LOG_INFO("Checkpoint complete: job=%s ok=%d", jid.c_str(), (int)ok);
  };

  if (auto r = ckpt_mgr_.take_checkpoint(notif.job_id, notif.vram_bytes, cb); !r) {
    return Error{"Checkpoint failed: " + r.error().msg};
  }


  auto resp_res = signal_cb_(notif);
  if (!resp_res) {
    return Error{"Coordinator signal failed: " + resp_res.error().msg};
  }
  const ReclaimResponse& resp = resp_res.value();
  LOG_INFO("ReclaimHandler: coordinator assigned dst=%s:%d bps=%.0f",
           resp.dst_node.c_str(), resp.dst_port, resp.allocated_bps);


  auto meta_res = ckpt_mgr_.latest_meta(notif.job_id);
  if (!meta_res) {
    return Error{"No checkpoint meta for " + notif.job_id};
  }

  MigrationParams params;
  params.job_id          = notif.job_id;
  params.dst_host        = resp.dst_node;
  params.dst_port        = resp.dst_port;
  params.checkpoint_path = meta_res.value().path;
  params.allocated_bps   = resp.allocated_bps;
  params.deadline_s      = resp.deadline_s;
  params.resume_seq      = 0;

  std::atomic<bool> cancel{false};
  TransferProgressCb prog_cb = [&notif](uint64_t b) {
    LOG_DEBUG("Migration progress: job=%s bytes=%llu",
              notif.job_id.c_str(), (unsigned long long)b);
  };

  if (auto r = mig_client_.send(params, prog_cb, cancel); !r) {
    return Error{"Migration send failed: " + r.error().msg};
  }


  ckpt_mgr_.purge(notif.job_id);
  LOG_INFO("ReclaimHandler: reclaim complete for job %s", notif.job_id.c_str());
  return ok_void();
}

}
}
