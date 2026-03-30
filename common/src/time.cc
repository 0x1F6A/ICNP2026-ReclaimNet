


#include "reclaimnet/time.h"

#include <cstdio>
#include <ctime>
#include <array>

namespace reclaimnet {



std::string format_duration_ns(uint64_t ns) {
  std::array<char, 64> buf{};
  if (ns < 1'000ULL) {
    snprintf(buf.data(), buf.size(), "%lluns", (unsigned long long)ns);
  } else if (ns < 1'000'000ULL) {
    snprintf(buf.data(), buf.size(), "%.3fµs", (double)ns / 1e3);
  } else if (ns < 1'000'000'000ULL) {
    snprintf(buf.data(), buf.size(), "%.3fms", (double)ns / 1e6);
  } else {
    snprintf(buf.data(), buf.size(), "%.3fs", (double)ns / 1e9);
  }
  return buf.data();
}



std::string format_timestamp_ns(int64_t unix_ns) {
  std::array<char, 64> buf{};
  time_t  sec = static_cast<time_t>(unix_ns / 1'000'000'000LL);
  int     ms  = static_cast<int>((unix_ns % 1'000'000'000LL) / 1'000'000LL);
  struct tm tm_utc{};
#ifdef _WIN32
  gmtime_s(&tm_utc, &sec);
#else
  gmtime_r(&sec, &tm_utc);
#endif
  snprintf(buf.data(), buf.size(),
           "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
           tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
           tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec, ms);
  return buf.data();
}

}
