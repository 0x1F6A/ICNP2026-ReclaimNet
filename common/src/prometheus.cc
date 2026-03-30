


#include "reclaimnet/prometheus.h"

#include <sstream>
#include <algorithm>
#include <iomanip>
#include <cmath>

namespace reclaimnet {



std::string LabelSet::to_string() const {
  if (labels.empty()) return "";
  std::ostringstream oss;
  oss << "{";
  for (size_t i = 0; i < labels.size(); ++i) {
    if (i > 0) oss << ",";
    oss << labels[i].first << "=\"" << labels[i].second << "\"";
  }
  oss << "}";
  return oss.str();
}



std::string Counter::serialize() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " counter\n";
  oss << name_ << labels_.to_string() << " " << std::fixed << std::setprecision(3) << get() << "\n";
  return oss.str();
}



std::string Gauge::serialize() const {
  std::ostringstream oss;
  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " gauge\n";
  oss << name_ << labels_.to_string() << " " << std::fixed << std::setprecision(6) << get() << "\n";
  return oss.str();
}



Histogram::Histogram(std::string name, std::string help,
                     std::vector<double> boundaries,
                     LabelSet labels)
  : name_(std::move(name)), help_(std::move(help)),
    labels_(std::move(labels)),
    boundaries_(std::move(boundaries)),
    bucket_counts_(boundaries_.size() + 1, 0) {}

void Histogram::observe(double value) noexcept {
  std::lock_guard<std::mutex> lk(mu_);

  for (size_t i = 0; i < boundaries_.size(); ++i) {
    if (value <= boundaries_[i]) {
      bucket_counts_[i]++;
      count_++;
      sum_ += value;
      return;
    }
  }

  bucket_counts_.back()++;
  count_++;
  sum_ += value;
}

std::string Histogram::serialize() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::ostringstream oss;
  const std::string ls = labels_.to_string();

  oss << "# HELP " << name_ << " " << help_ << "\n";
  oss << "# TYPE " << name_ << " histogram\n";

  uint64_t cumulative = 0;
  for (size_t i = 0; i < boundaries_.size(); ++i) {
    cumulative += bucket_counts_[i];

    std::string le_label;
    if (ls.empty()) {
      le_label = "{le=\"" + std::to_string(boundaries_[i]) + "\"}";
    } else {
      std::string base = ls.substr(0, ls.size() - 1);
      le_label = base + ",le=\"" + std::to_string(boundaries_[i]) + "\"}";
    }
    oss << name_ << "_bucket" << le_label << " " << cumulative << "\n";
  }

  {
    cumulative += bucket_counts_.back();
    std::string inf_label = ls.empty() ? "{le=\"+Inf\"}" : (ls.substr(0, ls.size()-1) + ",le=\"+Inf\"}");
    oss << name_ << "_bucket" << inf_label << " " << cumulative << "\n";
  }

  oss << name_ << "_count" << ls << " " << count_ << "\n";
  oss << name_ << "_sum"   << ls << " " << std::fixed << std::setprecision(6) << sum_ << "\n";
  return oss.str();
}



Counter& MetricRegistry::counter(std::string name, std::string help, LabelSet labels) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = counters_.find(name);
  if (it == counters_.end()) {
    auto ptr = std::make_unique<Counter>(name, std::move(help), std::move(labels));
    Counter& ref = *ptr;
    counters_.emplace(std::move(name), std::move(ptr));
    return ref;
  }
  return *it->second;
}

Gauge& MetricRegistry::gauge(std::string name, std::string help, LabelSet labels) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = gauges_.find(name);
  if (it == gauges_.end()) {
    auto ptr = std::make_unique<Gauge>(name, std::move(help), std::move(labels));
    Gauge& ref = *ptr;
    gauges_.emplace(std::move(name), std::move(ptr));
    return ref;
  }
  return *it->second;
}

Histogram& MetricRegistry::histogram(std::string name, std::string help,
                                      std::vector<double> bounds, LabelSet labels) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = histograms_.find(name);
  if (it == histograms_.end()) {
    auto ptr = std::make_unique<Histogram>(name, std::move(help), std::move(bounds), std::move(labels));
    Histogram& ref = *ptr;
    histograms_.emplace(std::move(name), std::move(ptr));
    return ref;
  }
  return *it->second;
}

std::string MetricRegistry::render() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::ostringstream oss;
  for (auto& [name, c] : counters_)    oss << c->serialize() << "\n";
  for (auto& [name, g] : gauges_)      oss << g->serialize() << "\n";
  for (auto& [name, h] : histograms_)  oss << h->serialize() << "\n";
  return oss.str();
}

}
