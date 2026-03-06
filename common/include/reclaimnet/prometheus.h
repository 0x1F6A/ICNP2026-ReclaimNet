/// @file prometheus.h
/// @brief Lightweight Prometheus text-format metrics registry.
///
/// Implements the /metrics exposition format (text/plain; version=0.0.4).
/// Provides Counter, Gauge, and Histogram types.  All operations are
/// thread-safe via a shared mutex on the registry.

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>
#include <functional>
#include <cmath>

namespace reclaimnet {

// Forward declarations
class Counter;
class Gauge;
class Histogram;

// ── MetricFamily ─────────────────────────────────────────────────────────────

/// A labelled metric instance (single time-series).
struct LabelSet {
  std::vector<std::pair<std::string,std::string>> labels;

  std::string to_string() const;
};

// ── Counter ──────────────────────────────────────────────────────────────────

/// Monotonically increasing counter.
class Counter {
 public:
  explicit Counter(std::string name, std::string help, LabelSet labels = {})
    : name_(std::move(name)), help_(std::move(help)),
      labels_(std::move(labels)), value_(0) {}

  void inc(double delta = 1.0) noexcept { value_.fetch_add(static_cast<uint64_t>(delta * 1000)); }

  [[nodiscard]] double get() const noexcept { return static_cast<double>(value_.load()) / 1000.0; }

  std::string serialize() const;

  const std::string& name() const noexcept { return name_; }
  const std::string& help() const noexcept { return help_; }

 private:
  std::string           name_;
  std::string           help_;
  LabelSet              labels_;
  std::atomic<uint64_t> value_;
};

// ── Gauge ────────────────────────────────────────────────────────────────────

/// Instantaneous gauge metric (can go up or down).
class Gauge {
 public:
  explicit Gauge(std::string name, std::string help, LabelSet labels = {})
    : name_(std::move(name)), help_(std::move(help)), labels_(std::move(labels)) {}

  void set(double v) noexcept {
    std::unique_lock<std::mutex> lk(mu_);
    value_ = v;
  }
  void inc(double delta = 1.0) noexcept {
    std::unique_lock<std::mutex> lk(mu_);
    value_ += delta;
  }
  void dec(double delta = 1.0) noexcept {
    std::unique_lock<std::mutex> lk(mu_);
    value_ -= delta;
  }

  [[nodiscard]] double get() const noexcept {
    std::unique_lock<std::mutex> lk(mu_);
    return value_;
  }

  std::string serialize() const;

  const std::string& name() const noexcept { return name_; }
  const std::string& help() const noexcept { return help_; }

 private:
  std::string         name_;
  std::string         help_;
  LabelSet            labels_;
  mutable std::mutex  mu_;
  double              value_ = 0.0;
};

// ── Histogram ────────────────────────────────────────────────────────────────

/// Observation histogram with configurable bucket boundaries.
class Histogram {
 public:
  Histogram(std::string name, std::string help,
            std::vector<double> boundaries,
            LabelSet labels = {});

  void observe(double value) noexcept;

  std::string serialize() const;

  const std::string& name() const noexcept { return name_; }

 private:
  std::string           name_;
  std::string           help_;
  LabelSet              labels_;
  std::vector<double>   boundaries_;
  mutable std::mutex    mu_;
  std::vector<uint64_t> bucket_counts_;
  uint64_t              count_  = 0;
  double                sum_    = 0.0;
};

// ── Registry ─────────────────────────────────────────────────────────────────

/// Central metric registry.  Typically used as a singleton.
class MetricRegistry {
 public:
  static MetricRegistry& instance() {
    static MetricRegistry inst;
    return inst;
  }

  /// Register and return a counter.  Idempotent by name.
  Counter&   counter(std::string name, std::string help, LabelSet labels = {});
  Gauge&     gauge(std::string name, std::string help, LabelSet labels = {});
  Histogram& histogram(std::string name, std::string help,
                       std::vector<double> bounds, LabelSet labels = {});

  /// Render all metrics in Prometheus text format.
  std::string render() const;

 private:
  MetricRegistry() = default;

  mutable std::mutex mu_;
  std::unordered_map<std::string, std::unique_ptr<Counter>>   counters_;
  std::unordered_map<std::string, std::unique_ptr<Gauge>>     gauges_;
  std::unordered_map<std::string, std::unique_ptr<Histogram>> histograms_;
};

// ── Default histogram boundaries ─────────────────────────────────────────────

/// Checkpoint interval boundaries (seconds).
inline std::vector<double> checkpoint_interval_buckets() {
  return {1, 5, 10, 30, 60, 120, 300, 600};
}

/// Migration transfer time boundaries (seconds).
inline std::vector<double> migration_time_buckets() {
  return {0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0};
}

/// Bandwidth allocation buckets (Mbps).
inline std::vector<double> bw_allocation_buckets() {
  return {10e6, 50e6, 100e6, 250e6, 500e6, 1000e6, 2500e6, 10000e6};
}

}  // namespace reclaimnet
