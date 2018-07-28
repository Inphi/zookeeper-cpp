#pragma once

#include <boost/asio/buffer.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>
#include "owned_buffer.h"
#include "proto.h"
#include "error.h"

#include <iostream> // debug
#include <boost/utility/string_ref.hpp> // debug

namespace zookeeper {

// debug
void trace(boost::string_ref msg) { std::cerr << msg << std::endl; }

template <typename Stream, typename Handler>
struct zk_write_op {
  Handler h_;
  Stream& s_;
  owned_buffer& b_;

  bool done = false;

  template <typename DeducedHandler>
  zk_write_op(DeducedHandler&& h, Stream& s, owned_buffer& b)
      : h_(std::forward<DeducedHandler>(h)), s_(s), b_(b) {}

  zk_write_op(zk_write_op&&) = default;
  zk_write_op& operator=(zk_write_op&&) = default;

  void operator()(const boost::system::error_code& ec, std::size_t n) {
    if (!done) {
      done = true;
      trace("zk_write: writing");
      boost::asio::async_write(s_, boost::asio::buffer(b_.data(), b_.size()),
                               std::move(*this));
    } else {
      trace("zk_write: done");
      h_(ec);
    }
  }
};

template <typename Stream, typename Handler>
struct zk_read_op {
  Handler h_;
  Stream& s_;
  std::shared_ptr<std::string> b_;

  int state_ = 0;

  template <typename DeducedHandler>
  zk_read_op(DeducedHandler&& h, Stream& s, std::shared_ptr<std::string> b)
      : h_(std::forward<DeducedHandler>(h)), s_(s), b_(std::move(b)) {
    b_->reserve(4 + 1024);
  }

  zk_read_op(zk_read_op&&) = default;
  zk_read_op& operator=(zk_read_op&&) = default;

  void operator()(const boost::system::error_code& ec, std::size_t n) {
    switch (state_) {
      case 0:
        state_ = 1;
        trace("zk_read: receive hdr");
        b_->resize(4);
        boost::asio::async_read(s_, boost::asio::buffer(&(*b_)[0], 4),
                                std::move(*this));
        break;
      case 1:
        trace("zk_read: state 1");
        if (ec) {
          h_(ec);
        } else {
          union {
            int32_t n;
            char b[4];
          } u;
          std::copy_n(b_->data(), 4, u.b);
          const auto siz = be32toh(u.n);
          b_->resize(siz);
          state_ = 2;
          trace("zk_read: receive body");
          s_.async_receive(boost::asio::buffer(&(*b_)[0], b_->size()),
                           std::move(*this));
        }
        break;
      case 2:
        trace("zk_read: state 2");
        h_(ec);
        break;
    }
  }
};

template <class Stream, class Handler>
class zk_op {
  Stream& s_;
  Handler h_;

  std::shared_ptr<owned_buffer> b_;
  std::shared_ptr<std::string> rb_;

  enum class state {
    sending,
    receiving,
    done,
  } st_{state::sending};

 public:
  template <typename DeducedHandler>
  zk_op(DeducedHandler&& h, Stream& s, op_code opcode,
        std::shared_ptr<owned_buffer> b, std::shared_ptr<std::string> rb)
      : s_(s),
        h_(std::forward<DeducedHandler>(h)),
        b_(std::move(b)),
        rb_(std::move(rb)) {}

  void operator()(boost::system::error_code ec) {
    switch (st_) {
      case state::sending:
        st_ = state::receiving;
        trace("zk_op: writing");
        zk_write(s_, *b_, std::move(*this));
        break;
      case state::receiving:
        if (ec) {
          h_(ec);
          return;
        }
        b_->reset();  // reclaim space asap
        trace("zk_op: reading");
        st_ = state::done;
        zk_read(s_, rb_, std::move(*this));
        break;
      case state::done:
        h_(ec);
        break;
    }
  }
};

template <class AsyncReadStream, class ReadHandler>
auto zk_read(AsyncReadStream& stream, std::shared_ptr<std::string> buf,
             ReadHandler&& handler) {
  boost::asio::async_completion<ReadHandler, void(boost::system::error_code)>
      init{handler};
  zk_read_op<AsyncReadStream,
             typename boost::asio::handler_type<
                 ReadHandler, void(boost::system::error_code)>::type>{
      init.completion_handler, stream, buf}(boost::system::error_code{}, 0);
  return init.result.get();
}

template <class AsyncReadStream, class ReadHandler>
auto zk_write(AsyncReadStream& stream, owned_buffer& buf,
              ReadHandler&& handler) {
  boost::asio::async_completion<ReadHandler, void(boost::system::error_code)>
      init{handler};
  zk_write_op<AsyncReadStream,
              typename boost::asio::handler_type<
                  ReadHandler, void(boost::system::error_code)>::type>{
      init.completion_handler, stream, buf}(boost::system::error_code{}, 0);
  return init.result.get();
}

template <class AsyncStream, class ReadHandler>
auto zk(AsyncStream& stream, op_code oc, std::shared_ptr<owned_buffer> r,
        std::shared_ptr<std::string> rb, ReadHandler&& handler) {
  boost::asio::async_completion<ReadHandler, void(boost::system::error_code)>
      init{handler};
  zk_op<AsyncStream, typename boost::asio::handler_type<
                         ReadHandler, void(boost::system::error_code)>::type>{
      init.completion_handler, stream, oc, std::move(r),
      std::move(rb)}(boost::system::error_code{});
  return init.result.get();
}

}  // end namespace zookeeper
