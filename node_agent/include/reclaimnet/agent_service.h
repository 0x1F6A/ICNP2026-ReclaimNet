#pragma once
#include "reclaimnet/result.h"
#include <memory>
namespace reclaimnet::agent {
class NodeAgent;
class AgentServiceHost {
public:
  explicit AgentServiceHost(NodeAgent &agent);
  ~AgentServiceHost();
  VoidResult start();
  void stop();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}
