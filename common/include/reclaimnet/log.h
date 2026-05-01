

#pragma once

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <mutex>
#include <string>

namespace reclaimnet {

enum class LogLevel : int {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERR = 3,
  FATAL = 4,
};

struct SourceLocation {
  const char *file;
  int line;
  const char *func;
};

class Logger {
public:
  static Logger &instance() noexcept {
    static Logger inst;
    return inst;
  }

  void set_level(LogLevel l) noexcept { level_.store(l); }
  [[nodiscard]] LogLevel level() const noexcept { return level_.load(); }

  void log(LogLevel sev, const SourceLocation &loc, const char *fmt,
           ...) noexcept;

  void flush() noexcept { fflush(stderr); }

private:
  Logger() = default;
  std::atomic<LogLevel> level_{LogLevel::INFO};
  std::mutex mu_;

  static const char *level_str(LogLevel l) noexcept;
};

#define RNET_LOG_IMPL(sev, fmt, ...)                                           \
  do {                                                                         \
    ::reclaimnet::Logger::instance().log(::reclaimnet::LogLevel::sev,          \
                                         {__FILE__, __LINE__, __func__},       \
                                         fmt, ##__VA_ARGS__);                  \
  } while (false)

#define LOG_DEBUG(fmt, ...) RNET_LOG_IMPL(DEBUG, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) RNET_LOG_IMPL(INFO, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...) RNET_LOG_IMPL(WARN, fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt, ...) RNET_LOG_IMPL(ERR, fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...)                                                    \
  do {                                                                         \
    RNET_LOG_IMPL(FATAL, fmt, ##__VA_ARGS__);                                  \
    ::abort();                                                                 \
  } while (false)

class ScopeLog {
public:
  explicit ScopeLog(const char *name) noexcept : name_(name) {
    LOG_DEBUG("→ %s", name_);
  }
  ~ScopeLog() noexcept { LOG_DEBUG("← %s", name_); }

private:
  const char *name_;
};

}
