/// @file log.h
/// @brief Structured levelled logging with source location annotation.
///
/// Provides LOG_DEBUG / LOG_INFO / LOG_WARN / LOG_ERR / LOG_FATAL macros
/// that embed file, line, and function context automatically.  The backend
/// writes to stderr with an ISO-8601 UTC timestamp prefix.

#pragma once

#include <string>
#include <cstdio>
#include <cstdarg>
#include <atomic>
#include <mutex>

namespace reclaimnet {

/// Log severity levels (must remain ordered ascending).
enum class LogLevel : int {
  DEBUG = 0,
  INFO  = 1,
  WARN  = 2,
  ERR   = 3,
  FATAL = 4,
};

/// Source location bundle injected by macros.
struct SourceLocation {
  const char* file;
  int         line;
  const char* func;
};

/// Singleton logger – configured once at startup.
class Logger {
 public:
  static Logger& instance() noexcept {
    static Logger inst;
    return inst;
  }

  void set_level(LogLevel l) noexcept { level_.store(l); }
  [[nodiscard]] LogLevel level() const noexcept { return level_.load(); }

  /// Core log function.  Thread-safe.
  void log(LogLevel sev, const SourceLocation& loc,
           const char* fmt, ...) noexcept;

  /// Flush (no-op for stderr backend, but useful for tests).
  void flush() noexcept { fflush(stderr); }

 private:
  Logger() = default;
  std::atomic<LogLevel> level_{LogLevel::INFO};
  std::mutex            mu_;

  static const char* level_str(LogLevel l) noexcept;
};

// ── Public log macros ─────────────────────────────────────────────────────────

/// Internal helper – do not call directly.
#define RNET_LOG_IMPL(sev, fmt, ...)                            \
  do {                                                          \
    ::reclaimnet::Logger::instance().log(                       \
      ::reclaimnet::LogLevel::sev,                              \
      {__FILE__, __LINE__, __func__},                           \
      fmt, ##__VA_ARGS__);                                      \
  } while (false)

#define LOG_DEBUG(fmt, ...) RNET_LOG_IMPL(DEBUG, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  RNET_LOG_IMPL(INFO,  fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  RNET_LOG_IMPL(WARN,  fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt, ...)   RNET_LOG_IMPL(ERR,   fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) do { RNET_LOG_IMPL(FATAL, fmt, ##__VA_ARGS__); \
                                 ::abort(); } while (false)

// ── RAII scope logger ─────────────────────────────────────────────────────────

/// Logs entry and exit of a named scope at DEBUG level.
class ScopeLog {
 public:
  explicit ScopeLog(const char* name) noexcept : name_(name) {
    LOG_DEBUG("→ %s", name_);
  }
  ~ScopeLog() noexcept { LOG_DEBUG("← %s", name_); }
 private:
  const char* name_;
};

}  // namespace reclaimnet
