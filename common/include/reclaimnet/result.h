/// @file result.h
/// @brief Lightweight Result<T,E> monad for error propagation without exceptions.
///
/// Modelled after Rust's Result<T, E>.  Used throughout ReclaimNet to surface
/// configuration errors, admission rejections, and eBPF load failures without
/// throwing exceptions across gRPC boundaries.

#pragma once

#include <string>
#include <stdexcept>
#include <functional>
#include <variant>
#include <optional>
#include <type_traits>

namespace reclaimnet {

// ── Error types ───────────────────────────────────────────────────────────────

/// Base error type carrying a human-readable message and optional error code.
struct Error {
  std::string message;
  int         code = 0;

  explicit Error(std::string msg, int c = 0)
    : message(std::move(msg)), code(c) {}

  bool operator==(const Error& o) const noexcept {
    return code == o.code && message == o.message;
  }
};

// ── Result<T, E> ──────────────────────────────────────────────────────────────

/// @brief A value-or-error wrapper.
///
/// @tparam T  Success type.
/// @tparam E  Error type (default: reclaimnet::Error).
///
/// Usage:
/// @code
///   Result<uint64_t> r = compute_bps(job);
///   if (!r) { LOG_ERR("{}", r.error().message); return r.error(); }
///   uint64_t bps = r.value();
/// @endcode
template <typename T, typename E = Error>
class Result {
 public:
  using value_type = T;
  using error_type = E;

  // ── Constructors ────────────────────────────────────────────────────────────

  /// Construct a successful result.
  static Result ok(T v) {
    Result r;
    r.data_ = std::move(v);
    return r;
  }

  /// Construct a failed result.
  static Result err(E e) {
    Result r;
    r.data_ = std::move(e);
    return r;
  }

  /// Implicit conversion from value (allows `return value;`).
  /* implicit */ Result(T v) : data_(std::move(v)) {}  // NOLINT

  /// Implicit conversion from error (allows `return Error{"msg"};`).
  /* implicit */ Result(E e) : data_(std::move(e)) {}  // NOLINT

  // ── Observers ───────────────────────────────────────────────────────────────

  /// Returns true if this holds a success value.
  [[nodiscard]] bool is_ok()  const noexcept { return std::holds_alternative<T>(data_); }

  /// Returns true if this holds an error.
  [[nodiscard]] bool is_err() const noexcept { return std::holds_alternative<E>(data_); }

  /// Equivalent to is_ok().
  explicit operator bool() const noexcept { return is_ok(); }

  /// Return the held value; throws std::runtime_error if this is an error.
  [[nodiscard]] const T& value() const & {
    if (!is_ok()) throw std::runtime_error("Result::value() called on error");
    return std::get<T>(data_);
  }

  [[nodiscard]] T& value() & {
    if (!is_ok()) throw std::runtime_error("Result::value() called on error");
    return std::get<T>(data_);
  }

  [[nodiscard]] T value() && {
    if (!is_ok()) throw std::runtime_error("Result::value() called on error");
    return std::get<T>(std::move(data_));
  }

  /// Return the held error; throws if this is a success.
  [[nodiscard]] const E& error() const & {
    if (!is_err()) throw std::runtime_error("Result::error() called on ok");
    return std::get<E>(data_);
  }

  [[nodiscard]] E& error() & {
    if (!is_err()) throw std::runtime_error("Result::error() called on ok");
    return std::get<E>(data_);
  }

  /// Return value or a default if this is an error.
  [[nodiscard]] T value_or(T def) const & {
    return is_ok() ? std::get<T>(data_) : std::move(def);
  }

  // ── Monadic operations ───────────────────────────────────────────────────────

  /// Transform the success value; propagate error unchanged.
  template <typename F>
  auto map(F&& f) const -> Result<std::invoke_result_t<F, T>, E> {
    using U = std::invoke_result_t<F, T>;
    if (is_ok()) return Result<U, E>::ok(std::forward<F>(f)(std::get<T>(data_)));
    return Result<U, E>::err(std::get<E>(data_));
  }

  /// Chain another Result-returning function (flat-map / and_then).
  template <typename F>
  auto and_then(F&& f) const -> std::invoke_result_t<F, T> {
    if (is_ok()) return std::forward<F>(f)(std::get<T>(data_));
    return std::invoke_result_t<F, T>::err(std::get<E>(data_));
  }

  /// Apply a function to the error, leaving success unchanged.
  template <typename F>
  auto map_err(F&& f) const -> Result<T, std::invoke_result_t<F, E>> {
    using E2 = std::invoke_result_t<F, E>;
    if (is_err()) return Result<T, E2>::err(std::forward<F>(f)(std::get<E>(data_)));
    return Result<T, E2>::ok(std::get<T>(data_));
  }

 private:
  Result() = default;
  std::variant<T, E> data_;
};

// ── Convenience aliases ───────────────────────────────────────────────────────

/// Result with no success payload (indicates success/failure only).
using VoidResult = Result<std::monostate, Error>;

/// Construct a successful void result.
inline VoidResult ok_void() { return VoidResult::ok(std::monostate{}); }

/// Construct a failing void result.
inline VoidResult err_void(std::string msg, int code = 0) {
  return VoidResult::err(Error{std::move(msg), code});
}

}  // namespace reclaimnet
