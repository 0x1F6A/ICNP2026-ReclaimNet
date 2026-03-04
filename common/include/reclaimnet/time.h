/// @file time.h
/// @brief Wall-clock and monotonic clock utilities with nanosecond resolution.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace reclaimnet {

// ── Epoch helpers ─────────────────────────────────────────────────────────────

/// Return the current wall-clock time as Unix nanoseconds.
inline int64_t now_ns() noexcept {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(
    system_clock::now().time_since_epoch()).count();
}

/// Return the current wall-clock time as Unix microseconds.
inline int64_t now_us() noexcept { return now_ns() / 1000LL; }

/// Return the current wall-clock time as Unix milliseconds.
inline int64_t now_ms() noexcept { return now_ns() / 1'000'000LL; }

/// Return the current wall-clock time as Unix seconds (floating point).
inline double now_s() noexcept {
  return static_cast<double>(now_ns()) * 1e-9;
}

// ── Monotonic clock ───────────────────────────────────────────────────────────

/// Return a monotonic timestamp in nanoseconds (for internal measurements).
inline int64_t mono_ns() noexcept {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(
    steady_clock::now().time_since_epoch()).count();
}

// ── Duration conversion helpers ───────────────────────────────────────────────

/// Convert nanoseconds integer to std::chrono::nanoseconds.
inline std::chrono::nanoseconds ns_to_duration(uint64_t ns) noexcept {
  return std::chrono::nanoseconds(ns);
}

/// Convert floating-point seconds to nanoseconds integer.
inline uint64_t seconds_to_ns(double s) noexcept {
  return static_cast<uint64_t>(s * 1e9);
}

/// Convert nanoseconds to floating-point seconds.
inline double ns_to_seconds(uint64_t ns) noexcept {
  return static_cast<double>(ns) * 1e-9;
}

// ── Human-readable formatting ─────────────────────────────────────────────────

/// Format a duration (nanoseconds) as a human-readable string, e.g. "1.234s" or "45.6ms".
std::string format_duration_ns(uint64_t ns);

/// Format a Unix-ns timestamp as ISO-8601 UTC string.
std::string format_timestamp_ns(int64_t unix_ns);

// ── Stopwatch ─────────────────────────────────────────────────────────────────

/// RAII stopwatch that measures elapsed nanoseconds.
class Stopwatch {
 public:
  Stopwatch() noexcept : start_(std::chrono::steady_clock::now()) {}

  /// Reset the stopwatch.
  void reset() noexcept { start_ = std::chrono::steady_clock::now(); }

  /// Return elapsed nanoseconds since construction or last reset().
  [[nodiscard]] uint64_t elapsed_ns() const noexcept {
    using namespace std::chrono;
    return static_cast<uint64_t>(
      duration_cast<nanoseconds>(steady_clock::now() - start_).count());
  }

  [[nodiscard]] double elapsed_s() const noexcept {
    return ns_to_seconds(elapsed_ns());
  }

 private:
  std::chrono::steady_clock::time_point start_;
};

// ── Rate estimator ────────────────────────────────────────────────────────────

/// Exponentially-weighted moving average for rate estimation.
/// Used by HazardEstimator and telemetry collectors.
class EWMARate {
 public:
  /// @param alpha Smoothing factor in (0,1].  Larger = faster response.
  explicit EWMARate(double alpha = 0.1) noexcept : alpha_(alpha) {}

  /// Submit a new sample and return the updated EWMA estimate.
  double update(double sample) noexcept {
    if (initialized_) {
      value_ = alpha_ * sample + (1.0 - alpha_) * value_;
    } else {
      value_ = sample;
      initialized_ = true;
    }
    return value_;
  }

  [[nodiscard]] double get() const noexcept { return value_; }
  [[nodiscard]] bool   ready() const noexcept { return initialized_; }
  void reset() noexcept { initialized_ = false; value_ = 0.0; }

 private:
  double alpha_;
  double value_      = 0.0;
  bool   initialized_ = false;
};

}  // namespace reclaimnet
