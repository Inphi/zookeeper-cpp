#pragma once

#include <type_traits>
#include <system_error>
#include <boost/variant.hpp>

namespace zookeeper {

template<typename T>
class result {
  static_assert(!std::is_same<std::decay_t<T>,std::error_code>::value, "");

public:
  result(const result&) = default;
  result& operator=(const result&) = default;

  result() = default;
  result(result&&) = default;
  result& operator=(result&&) = default;

  static result make_error(std::error_code ec) {
    result r;
    r.data_ = std::move(ec);
    return r;
  }

  template <typename... Args>
  static result make_ok(Args&&... args) {
    return boost::variant<std::error_code, T>{T(std::forward<Args>(args)...)};
  }

  bool is_err() const { return data_.which() == 1; }

  bool is_ok() const { return data_.which() == 0; }

  // preCond: this->is_ok()
  T& ok() { return boost::get<T>(data_); }

  // preCond: this->is_ok()
  const T& ok() const { return boost::get<T>(data_); }

  // preCond: this->is_err()
  std::error_code& err() { return boost::get<std::error_code>(data_); }

  // preCond: this->is_err()
  const std::error_code& err() const {
    return boost::get<std::error_code>(data_);
  }

private:
  boost::variant<T, std::error_code> data_;

  result(boost::variant<std::error_code, T> &&d) : data_(std::move(d)) {}
};


template <>
class result<void> {
public:
  result() = default;
  result(const result&) = default;
  result& operator=(const result&) = default;

  result(result&&) = default;
  result& operator=(result&&) = default;

  static result<void> make_error(std::error_code ec) {
    return boost::variant<std::error_code, boost::blank>{std::move(ec)};
  }

  static result make_ok() {
    return boost::variant<std::error_code, boost::blank>{boost::blank{}};
  }

  bool is_err() const { return data_.which() == 1; }

  bool is_ok() const { return data_.which() == 0; }

  // preCond: this->is_ok()
  void ok() { boost::get<boost::blank>(data_); }

  // preCond: this->is_ok()
  void ok() const { boost::get<boost::blank>(data_); }

  // preCond: this->is_err()
  std::error_code& err() { return boost::get<std::error_code>(data_); }

  // preCond: this->is_err()
  const std::error_code& err() const {
    return boost::get<std::error_code>(data_);
  }

private:
  boost::variant<boost::blank, std::error_code> data_;

  result(boost::variant<std::error_code, boost::blank> &&d) : data_(std::move(d)) {}
};

} // end namespace zookeeper
