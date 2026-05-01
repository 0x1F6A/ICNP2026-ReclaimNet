

#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <array>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>

namespace reclaimnet {

const char *Logger::level_str(LogLevel l) noexcept {
  switch (l) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO ";
  case LogLevel::WARN:
    return "WARN ";
  case LogLevel::ERR:
    return "ERROR";
  case LogLevel::FATAL:
    return "FATAL";
  default:
    return "?????";
  }
}

void Logger::log(LogLevel sev, const SourceLocation &loc, const char *fmt,
                 ...) noexcept {
  if (sev < level_.load(std::memory_order_relaxed))
    return;

  std::array<char, 2048> user_buf{};
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(user_buf.data(), user_buf.size(), fmt, ap);
  va_end(ap);

  std::array<char, 64> ts_buf{};
  {
    int64_t ns = now_ns();
    time_t sec = static_cast<time_t>(ns / 1'000'000'000LL);
    int ms = static_cast<int>((ns % 1'000'000'000LL) / 1'000'000LL);
    struct tm tm_utc{};
#ifdef _WIN32
    gmtime_s(&tm_utc, &sec);
#else
    gmtime_r(&sec, &tm_utc);
#endif
    snprintf(ts_buf.data(), ts_buf.size(),
             "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm_utc.tm_year + 1900,
             tm_utc.tm_mon + 1, tm_utc.tm_mday, tm_utc.tm_hour, tm_utc.tm_min,
             tm_utc.tm_sec, ms);
  }

  const char *file = loc.file;
  const char *slash = strrchr(file, '/');
  if (slash)
    file = slash + 1;
#ifdef _WIN32
  const char *bslash = strrchr(file, '\\');
  if (bslash)
    file = bslash + 1;
#endif

  std::lock_guard<std::mutex> lk(mu_);
  fprintf(stderr, "[%s] [%s] [%s:%d %s] %s\n", ts_buf.data(), level_str(sev),
          file, loc.line, loc.func, user_buf.data());
}

}
