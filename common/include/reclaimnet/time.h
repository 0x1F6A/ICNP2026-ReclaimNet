


#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace reclaimnet {




inline int64_t now_ns() noexcept {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(
    system_clock::now().time_since_epoch()).count();
}


inline int64_t now_us() noexcept { return now_ns() / 1000LL; }


inline int64_t now_ms() noexcept { return now_ns() / 1'000'000LL; }


inline double now_s() noexcept {
  return static_cast<double>(now_ns()) * 1e-9;
}




inline int64_t mono_ns() noexcept {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(
    steady_clock::now().time_since_epoch()).count();
}




inline std::chrono::nanoseconds ns_to_duration(uint64_t ns) noexcept {
  return std::chrono::nanoseconds(ns);
}


inline uint64_t seconds_to_ns(double s) noexcept {
  return static_cast<uint64_t>(s * 1e9);
}


inline double ns_to_seconds(uint64_t ns) noexcept {
  return static_cast<double>(ns) * 1e-9;
}




std::string format_duration_ns(uint64_t ns);


std::string format_timestamp_ns(int64_t unix_ns);




class Stopwatch {
 public:
  Stopwatch() noexcept : start_(std::chrono::steady_clock::now()) {}


  void reset() noexcept { start_ = std::chrono::steady_clock::now(); }


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





class EWMARate {
 public:

  explicit EWMARate(double alpha = 0.1) noexcept : alpha_(alpha) {}


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

}
