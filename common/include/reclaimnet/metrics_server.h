#pragma once
#include "reclaimnet/result.h"
#include <atomic>
#include <cstdint>
#include <thread>
namespace reclaimnet {
class MetricsServer {
public:
  ~MetricsServer();
  VoidResult start(uint16_t port);
  void stop();
  uint16_t port() const { return port_; }

private:
  int socket_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> running_{false};
  std::thread thread_;
};
}
