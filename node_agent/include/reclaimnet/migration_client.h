#pragma once







#include "reclaimnet/types.h"
#include "reclaimnet/result.h"

#include <string>
#include <functional>
#include <atomic>
#include <cstdint>

namespace reclaimnet {
namespace agent {


struct MigrationParams {
  JobId    job_id;
  std::string dst_host;
  uint16_t    dst_port;
  std::string checkpoint_path;
  double      allocated_bps;
  double      deadline_s;
  uint32_t    resume_seq;
};


using TransferProgressCb = std::function<void(uint64_t bytes_sent)>;


class MigrationClient {
public:
  MigrationClient() = default;



  VoidResult send(const MigrationParams& params,
                  const TransferProgressCb& progress_cb,
                  std::atomic<bool>& cancel_flag);



  static VoidResult receive(uint16_t port,
                             const std::string& dst_path,
                             uint64_t expected_bytes,
                             const TransferProgressCb& progress_cb);

private:
  static constexpr uint32_t MAGIC = 0x524E4D47u;
  static constexpr size_t   BLOCK = 65536;

  VoidResult send_block(int fd, uint32_t seq,
                        const char* data, size_t len);
  static VoidResult recv_all(int fd, char* buf, size_t n);
  static VoidResult send_all(int fd, const char* buf, size_t n);
};

}
}
