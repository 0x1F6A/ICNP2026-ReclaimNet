#pragma once
#include "reclaimnet/checkpoint_manager.h"
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
namespace reclaimnet::agent {
struct MigrationTlsConfig {
  std::string cert_path;
  std::string key_path;
  std::string ca_path;
  std::string server_name;
  double handshake_timeout_s = 10.0;
};
struct MigrationParams {
  JobId job_id;
  std::string dst_host;
  uint16_t dst_port = 0;
  std::string checkpoint_path;
  double allocated_bps = 0;
  double deadline_s = 0;
  uint32_t resume_seq = 0;
  int64_t deadline_ns = 0;
  CheckpointMeta checkpoint;
  bool restore_after_transfer = true;
  std::optional<JobRuntimeSpec> destination_runtime;
  std::function<double()> rate_provider;
  std::function<VoidResult(const FiveTuple &)> flow_started;
  std::function<void(const FiveTuple &)> flow_finished;
  std::string transfer_id;
  uint32_t rank = 0;
};
using TransferProgressCb = std::function<void(uint64_t)>;
using MigrationReceiveProgressCb = std::function<void(const JobId &, uint64_t)>;
using MigrationCommittedCb =
    std::function<VoidResult(const CheckpointMeta &, bool)>;
using MigrationValidateCb = std::function<VoidResult(
    const CheckpointMeta &, bool, const std::string &)>;
class MigrationClient {
public:
  explicit MigrationClient(MigrationTlsConfig tls = {});
  VoidResult send(const MigrationParams &params,
                  const TransferProgressCb &progress,
                  std::atomic<bool> &cancel_flag);

private:
  MigrationTlsConfig tls_;
};
class MigrationServer {
public:
  MigrationServer(MigrationTlsConfig tls, CheckpointManager &checkpoints,
                  MigrationReceiveProgressCb progress = {},
                  MigrationCommittedCb committed = {},
                  MigrationValidateCb validate = {});
  ~MigrationServer();
  VoidResult start(uint16_t port, const std::string &bind_address = "0.0.0.0");
  void stop();
  uint16_t port() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}
