#include "reclaimnet/reclaim_handler.h"
#include "reclaimnet/time.h"
namespace reclaimnet::agent {
ReclaimHandler::ReclaimHandler(CheckpointManager &checkpoints,
                               MigrationClient &client, ReclaimSignalCb signal,
                               ReclaimCallbacks callbacks)
    : checkpoints_(checkpoints), client_(client), signal_(std::move(signal)),
      callbacks_(std::move(callbacks)) {}
VoidResult ReclaimHandler::handle(const ReclaimNotification &notification) {
  cancel_ = false;
  auto fail = [&](Error error) -> VoidResult {
    if (callbacks_.aborted)
      callbacks_.aborted(notification.job_id, error);
    return error;
  };
  if (notification.type == ReclaimType::EMERGENCY) {
    if (!callbacks_.recover)
      return fail(
          Error{"No committed-replica recovery callback is configured"});
    auto result = callbacks_.recover(notification);
    auto stopped = checkpoints_.runtime().stop(notification.job_id);
    if (!result)
      return fail(result.error());
    return stopped;
  }
  if (notification.deadline_s <= 0)
    return fail(Error{"Scheduled reclaim requires positive notice"});
  const int64_t started = now_ns();
  auto response = signal_(notification);
  if (!response)
    return fail(response.error());
  const int64_t deadline =
      response.value().deadline_ns > 0
          ? response.value().deadline_ns
          : started + static_cast<int64_t>(notification.deadline_s * 1e9);
  if (notification.type == ReclaimType::SCHEDULED) {
    auto size = checkpoints_.runtime().state_size(notification.job_id);
    if (!size)
      return fail(size.error());
    if (!callbacks_.final_checkpoint_feasible ||
        !callbacks_.final_checkpoint_feasible(notification, size.value(),
                                              response.value())) {
      if (callbacks_.recover)
        return callbacks_.recover(notification);
      return fail(
          Error{"Final application checkpoint cannot fit the reclaim window"});
    }
    if (auto result = checkpoints_.take_checkpoint(
            notification.job_id, size.value(), {}, true, deadline);
        !result)
      return fail(result.error());
  }
  auto checkpoint = checkpoints_.latest_meta(notification.job_id);
  if (!checkpoint)
    return fail(checkpoint.error());
  MigrationParams params;
  params.job_id = notification.job_id;
  params.dst_host = response.value().dst_host.empty()
                        ? response.value().dst_node
                        : response.value().dst_host;
  params.dst_port = response.value().dst_port;
  params.checkpoint_path = checkpoint.value().path;
  params.checkpoint = checkpoint.value();
  params.allocated_bps = response.value().allocated_bps;
  params.deadline_ns = deadline;
  params.deadline_s = static_cast<double>(params.deadline_ns - now_ns()) / 1e9;
  params.destination_runtime = response.value().destination_runtime;
  if (callbacks_.configure_transfer)
    callbacks_.configure_transfer(params);
  auto progress = [&](uint64_t bytes) {
    if (callbacks_.progress) {
      auto result = callbacks_.progress(notification.job_id, bytes, false);
      if (!result)
        cancel_ = true;
    }
  };
  auto sent = client_.send(params, progress, cancel_);
  if (!sent) {
    if (notification.type == ReclaimType::SCHEDULED)
      checkpoints_.runtime().resume(notification.job_id);
    return fail(sent.error());
  }
  if (callbacks_.progress) {
    auto completed = callbacks_.progress(notification.job_id,
                                         checkpoint.value().size_bytes, true);
    if (!completed)
      return fail(completed.error());
  }
  if (auto result = checkpoints_.runtime().stop(notification.job_id); !result)
    return fail(result.error());
  return checkpoints_.purge(notification.job_id);
}
void ReclaimHandler::cancel() { cancel_ = true; }
}
