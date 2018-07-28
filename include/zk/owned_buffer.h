#pragma once

#include <cstdint>
#include <memory>
#include <algorithm>
#include <boost/asio/buffer.hpp>

namespace zookeeper {

class owned_buffer {
 public:
  using size_type = std::size_t;

  owned_buffer(std::size_t initial_cap = 64)
      : buf_(std::make_unique<char[]>(initial_cap)),
        off_(0),
        siz_(0),
        cap_(64) {}

  owned_buffer(owned_buffer&&) = default;
  owned_buffer& operator=(owned_buffer&&) = default;

  void grow(std::size_t n) {
    const auto rem = cap_ - siz_;
    if (rem >= n) return;

    const auto new_cap = cap_ + n;
    auto newbuf = std::make_unique<char[]>(new_cap);
    std::copy_n(buf_.get(), siz_, newbuf.get());
    buf_ = std::move(newbuf);
    cap_ = new_cap;
  }

  size_type write(boost::asio::const_buffer buf) {
    const auto bufsiz = boost::asio::buffer_size(buf);
    const auto* p = boost::asio::buffer_cast<const char*>(buf);

    const auto rem = cap_ - bufsiz;
    if (rem <= 0) {
      auto new_siz =
          std::max(static_cast<size_type>(2), static_cast<size_type>(cap_ * 2));
      while (new_siz < bufsiz) {
        new_siz *= 2;
      }
      auto newbuf = std::make_unique<char[]>(new_siz);
      std::copy_n(buf_.get(), siz_, newbuf.get());
      buf_ = std::move(newbuf);
    }

    std::copy_n(p, bufsiz, buf_.get() + siz_);

    siz_ += bufsiz;
    return bufsiz;
  }

  size_type read(boost::asio::mutable_buffer buf) {
    const auto sz = std::min(siz_, boost::asio::buffer_size(buf));
    auto* p = boost::asio::buffer_cast<char*>(buf);
    std::copy_n(buf_.get() + off_, sz, p);
    off_ += sz;
    return sz;
  }

  char* data() { return buf_.get(); }
  const char* data() const { return buf_.get(); }

  size_type size() const { return siz_; }
  size_type capacity() const { return cap_; }

  void reset() {
    buf_.reset();
    off_ = 0;
    siz_ = 0;
    cap_ = 0;
  }

 private:
  std::unique_ptr<char[]> buf_;
  std::ptrdiff_t off_;
  size_type siz_;
  size_type cap_;
};



} // end namespace zookeeper
