


#include "reclaimnet/agent.h"
#include "reclaimnet/config.h"
#include "reclaimnet/log.h"

#include <csignal>
#include <atomic>
#include <thread>
#include <chrono>
#include <cstdlib>

namespace {
std::atomic<bool> g_shutdown{false};
void sig_handler(int) { g_shutdown = true; }
}

int main(int argc, char* argv[]) {
  using namespace reclaimnet;
  using namespace reclaimnet::agent;

  const char* cfg_path = (argc > 1) ? argv[1] : "configs/agent.yaml";
  auto cfg_res = load_agent_config(cfg_path);
  if (!cfg_res) {
    LOG_FATAL("Cannot load config from %s: %s",
              cfg_path, cfg_res.error().msg.c_str());
    return 1;
  }
  auto cfg = cfg_res.value();
  if (auto v = validate_agent_config(cfg); !v) {
    LOG_FATAL("Invalid agent config: %s", v.error().msg.c_str());
    return 1;
  }

  Logger::instance().set_level(LogLevel::INFO);
  LOG_INFO("ReclaimNet node agent starting (node_id=%s)", cfg.node_id.c_str());

  NodeAgent agent(cfg);
  if (auto r = agent.start(); !r) {
    LOG_FATAL("Agent start failed: %s", r.error().msg.c_str());
    return 1;
  }

  std::signal(SIGINT,  sig_handler);
  std::signal(SIGTERM, sig_handler);

  while (!g_shutdown.load())
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

  LOG_INFO("Shutdown signal received");
  agent.stop();
  return 0;
}
