






#pragma once

#include <string>
#include <stdexcept>
#include <functional>
#include <variant>
#include <optional>
#include <type_traits>

namespace reclaimnet {




struct Error {
  std::string message;
  int         code = 0;

  explicit Error(std::string msg, int c = 0)
    : message(std::move(msg)), code(c) {}

  bool operator==(const Error& o) const noexcept {
    return code == o.code && message == o.message;
  }
};














template <typename T, typename E = Error>
class Result {
 public:
  using value_type = T;
  using error_type = E;




  static Result ok(T v) {
    Result r;
    r.data_ = std::move(v);
    return r;
  }


  static Result err(E e) {
    Result r;
    r.data_ = std::move(e);
    return r;
  }


   Result(T v) : data_(std::move(v)) {}


   Result(E e) : data_(std::move(e)) {}




  [[nodiscard]] bool is_ok()  const noexcept { return std::holds_alternative<T>(data_); }


  [[nodiscard]] bool is_err() const noexcept { return std::holds_alternative<E>(data_); }


  explicit operator bool() const noexcept { return is_ok(); }


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


  [[nodiscard]] const E& error() const & {
    if (!is_err()) throw std::runtime_error("Result::error() called on ok");
    return std::get<E>(data_);
  }

  [[nodiscard]] E& error() & {
    if (!is_err()) throw std::runtime_error("Result::error() called on ok");
    return std::get<E>(data_);
  }


  [[nodiscard]] T value_or(T def) const & {
    return is_ok() ? std::get<T>(data_) : std::move(def);
  }




  template <typename F>
  auto map(F&& f) const -> Result<std::invoke_result_t<F, T>, E> {
    using U = std::invoke_result_t<F, T>;
    if (is_ok()) return Result<U, E>::ok(std::forward<F>(f)(std::get<T>(data_)));
    return Result<U, E>::err(std::get<E>(data_));
  }


  template <typename F>
  auto and_then(F&& f) const -> std::invoke_result_t<F, T> {
    if (is_ok()) return std::forward<F>(f)(std::get<T>(data_));
    return std::invoke_result_t<F, T>::err(std::get<E>(data_));
  }


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




using VoidResult = Result<std::monostate, Error>;


inline VoidResult ok_void() { return VoidResult::ok(std::monostate{}); }


inline VoidResult err_void(std::string msg, int code = 0) {
  return VoidResult::err(Error{std::move(msg), code});
}

}
