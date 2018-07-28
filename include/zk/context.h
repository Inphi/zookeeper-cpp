#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/optional.hpp>
#include <boost/utility/string_ref.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <queue>
#include <vector>
#include "error.h"
#include "io.h"
#include "owned_buffer.h"
#include "proto.h"
#include "result.h"
#include "detail/log.h"

#define ZK_LOGGING 1 // debug

#ifdef ZK_LOGGING
#include <iostream>
#endif

namespace zookeeper {

// TODO: use asio_handler_invoke for ez and safe strand dispatch

using duration = std::chrono::milliseconds;

static bool path_valid(boost::string_ref path) {
  if (path.empty()) return false;
  if (path[0] != '/') return false;
  if (path.size() > 1 && path.back() == '/') return false;
  return true;
} 

class context : public std::enable_shared_from_this<context> {
  using clock = std::chrono::steady_clock;

  using op = std::function<void()>;
  using op_queue = std::queue<op>;

  enum class state {
    connected,
    disconnected,
    session_expired,
    connecting,
    not_connected,
  };

  struct addr {
    std::string host;
    unsigned short port;
  };

  // TODO: parse chroot
  static auto parse_quorum(std::string quorum) {
    std::pair<std::vector<addr>, boost::optional<std::string>> addrs_chroot;

    auto it = quorum.find_first_of("/");
    if (it != std::string::npos) {
      addrs_chroot.second = quorum.substr(it);
      if (!path_valid(*addrs_chroot.second)) {
        throw std::invalid_argument("invalid quorum chroot specified");
      }
      quorum = quorum.substr(0, it);
    }

    std::vector<std::string> hosts;
    boost::algorithm::split(hosts, quorum, boost::is_any_of(","));

    for (const auto& host : hosts) {
      std::vector<std::string> tokens;
      boost::algorithm::split(tokens, host, boost::is_any_of(":"));
      if (tokens.size() > 2) {
        throw std::invalid_argument("invalid quorum");
      }

      addr a;
      a.host = tokens[0];
      a.port = tokens.size() == 2 ? static_cast<unsigned short>(
                                        strtol(tokens[1].data(), nullptr, 10))
                                  : 2181u;
      addrs_chroot.first.emplace_back(std::move(a));
    }
    return addrs_chroot;
  }

  class addrs {
    // NOTE: we don't store as endpoints here to permit host migrations to
    // different ip addreses without restarting the context
    std::vector<addr> addrs_;
    std::size_t index = 0;

   public:
    addrs() = default;
    addrs(std::vector<addr> addrs) : addrs_(std::move(addrs)) {}

    const addr& get() {
      const auto& s = addrs_.at(index);
      if (index >= addrs_.size()) index = 0;
      return s;
    }
  };

  std::chrono::milliseconds timeout_;
  boost::asio::io_service::strand strand_;
  boost::asio::ip::tcp::socket sock_;
  boost::asio::ip::tcp::resolver resolver_;
  boost::asio::basic_waitable_timer<clock> timer_;
  addrs addrs_;
  boost::optional<std::string> chroot_;

  state state_{state::not_connected};

  struct session_state {
    std::int32_t protocol_version;
    duration timeout;
    std::int64_t session_id;
    bool read_only;
  };

  boost::optional<session_state> session_state_;
  op_queue q_;
  bool op_running_{false};
  std::atomic_int64_t xid_;

 public:
  static auto make(boost::asio::io_service& ios, std::string quorum,
                   duration timeout) {
    std::shared_ptr<context> ctx(new context{ios, std::move(quorum), timeout});
    ctx->init();
    return ctx;
  }

  ~context() { close(); }

  std::string resolve_path(boost::string_ref path) {
    if (chroot_) {
      if (path == "/") return *chroot_;
      else {
        if (!path_valid(path)) throw std::invalid_argument("invalid path");
        return *chroot_ + path.to_string();
      }
    }
    if (!path_valid(path)) throw std::invalid_argument("invalid path");
    return path.to_string();
  }

  template <class ReadHandler>
  auto create(boost::string_ref path, std::vector<char> data,
              std::vector<acl> acls, create_mode mode,
              ReadHandler&& handler) {
    using result_data = std::string;

    create_request req;
    try {
      req.path = resolve_path(path);
      req.acls = std::move(acls);
      req.data = std::move(data);
      req.flags = static_cast<std::int32_t>(mode);
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<create_response> && r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().path));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<create_request, create_response>(
        op_code::create, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto exists(boost::string_ref path, bool watch, ReadHandler&& handler) {
    using result_data = boost::optional<stat>;

    exists_request req;
    try {
      req.path = path.to_string();
      req.watch = watch;
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<exists_response> && r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().st));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<exists_request, exists_response>(
        op_code::exists, req, std::move(transform), std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto get_children(boost::string_ref path, bool watch, ReadHandler&& handler) {
    using result_data = std::vector<std::string>;

    get_children_request req;
    try {
      req.path = path.to_string();
      req.watch = watch;
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<get_children_response> && r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().children));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<get_children_request, get_children_response>(
        op_code::get_children, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto get_data(boost::string_ref path, bool watch, ReadHandler&& handler) {
    using result_data = std::pair<std::vector<char>, stat>;

    get_data_request req;
    try {
      req.path = path.to_string();
      req.watch = watch;
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<get_data_response> && r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(
            std::make_pair(std::move(r.ok().data), std::move(r.ok().st)));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<get_data_request, get_data_response>(
        op_code::get_data, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto set_data(boost::string_ref path, const std::vector<char>& data,
                boost::optional<std::int32_t> version, ReadHandler&& handler) {
    using result_data = stat;

    set_data_request req;
    try {
      req.path = path.to_string();
      req.data = data;
      req.version = version ? *version : -1;
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<set_data_response> && r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().st));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<get_data_request, get_data_response>(
        op_code::set_data, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto get_acl(boost::string_ref path, ReadHandler&& handler) {
    using result_data = std::pair<std::vector<acl>, stat>;

    get_acl_request req;
    try {
      req.path = path.to_string();
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<get_acl_response>&& r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().acls), std::move(r.ok().st));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<get_acl_request, get_acl_response>(
        op_code::get_acl, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto set_acl(boost::string_ref path, const std::vector<acl>& acls,
               boost::optional<std::int32_t> version, ReadHandler&& handler) {
    using result_data = std::pair<std::vector<acl>, stat>;

    set_acl_request req;
    try {
      req.path = path.to_string();
      req.acls = acls;
      req.version = version ? *version : -1;
    } catch (const std::invalid_argument&) {
      return dispatch_error<result_data>(error::bad_arguments,
                                         std::forward<ReadHandler>(handler));
    }

    auto transform = [](result<get_acl_response>&& r) -> auto {
      if (r.is_ok()) {
        return result<result_data>::make_ok(std::move(r.ok().acls), std::move(r.ok().st));
      } else {
        return result<result_data>::make_error(r.err());
      }
    };

    return dispatch<get_acl_request, get_acl_response>(
        op_code::set_acl, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  template <class ReadHandler>
  auto remove(boost::string_ref path, boost::optional<std::int32_t> version,
              ReadHandler&& handler) {
    delete_request req;
    req.path = path.to_string();
    req.version = version ? *version : -1;

    auto transform = [](result<empty_response> && r) -> auto {
      if (r.is_ok()) {
        return result<void>::make_ok();
      } else {
        return result<void>::make_error(r.err());
      }
    };

    return dispatch<delete_request, empty_response>(
        op_code::remove, req, std::move(transform),
        std::forward<ReadHandler>(handler));
  }

  boost::optional<std::int64_t> session_id() const { return boost::none; }

  boost::optional<duration> negotiated_timeout() const { return boost::none; }

  boost::optional<int32_t> protocol_version() const { return boost::none; }

 private:
  context(boost::asio::io_service& ios, std::string quorum,
          std::chrono::milliseconds timeout) :
        timeout_{timeout},
        strand_{ios},
        sock_{ios},
        resolver_{sock_.get_io_service()},
        timer_{ios},
        xid_{1} {
    auto addrs_chroot = parse_quorum(quorum);
    addrs_ = addrs_chroot.first;
    chroot_ = addrs_chroot.second;
  }

  void init() { start_connection(); }

  std::int32_t xid() {
    return static_cast<std::int32_t>(xid_.fetch_add(1, std::memory_order_relaxed));
  }

  template <typename Work>
  void defer(Work&& w) {
    strand_.dispatch([self = shared_from_this(), w = std::move(w)]() mutable {
      w(std::move(self));
    });
  }

  template <typename Response, typename Transform, typename Handler>
  struct wrap_handler {
    context& c_;
    std::shared_ptr<std::string> rb_;
    Transform t_;
    Handler h_;

    template <typename DeducedHandler>
    wrap_handler(context& c, std::shared_ptr<std::string> rb, Transform&& t,
                 DeducedHandler&& h)
        : c_(c),
          rb_(std::move(rb)),
          t_(std::forward<Transform>(t)),
          h_(std::forward<DeducedHandler>(h)) {}

    void operator()(boost::system::error_code ec) {
      c_.op_running_ = false;
      c_.do_operations();

      if (ec) {
        // TODO: more descriptive errors
        execute(result<Response>::make_error(
            make_error_code(error::connection_loss)));
        return;
      }

      auto cb = boost::asio::const_buffer(rb_->data(), rb_->size());
      auto rh = reply_header::deserialize(cb);
      detail::trace(rh);
      // TODO
      switch (rh.xid) {
        case -1:
          // watcher event
          break;
        case -2:
          // ping
          break;
        default:
          break;
      }

      if (rh.err != 0) {
        std::error_code s_ec;
        s_ec.assign(rh.err, _zk_ec);
        execute(result<Response>::make_error({rh.err, _zk_ec}));
        return;
      }

      auto response = Response::deserialize(cb);
      execute(result<Response>::make_ok(std::move(response)));
    }

    inline void execute(result<Response>&& r) {
      h_(t_(std::forward<result<Response>>(r)));
    }
  };

  template <typename ResultT, typename Handler>
  auto dispatch_error(error ec, Handler&& handler) {
    using result_type = result<ResultT>;
    boost::asio::async_completion<Handler, void(result_type)> init{handler};

    auto r = result_type::make_error(make_error_code(ec));
    defer([r = std::move(r), h = std::move(init.completion_handler)](
              auto self) mutable { h(std::forward<result_type>(r)); });

    return init.result.get();
  }

  template <typename Request, typename Response, typename Transform,
            typename Handler>
  auto dispatch(op_code oc, Request& r, Transform&& transform,
                Handler&& handler) {
    using result_type = result<Response>;
    using transform_result = decltype(transform(std::declval<result_type>()));
    boost::asio::async_completion<Handler, void(transform_result)> init{handler};

    auto b = std::make_shared<owned_buffer>();
    char* begin = b->data() + b->size();
    // TODO: owned_buffer::skip
    int32_t len = 0;  // placeholder
    b->write(boost::asio::buffer(reinterpret_cast<const char*>(&len), 4));

    request_header hdr;
    hdr.xid = xid();
    hdr.opcode = static_cast<int32_t>(oc);
    request_payload<Request> payload{hdr, std::forward<Request>(r)};
    payload.serialize(*b);
    len = b->data() + b->size() - begin - 4;
    const auto encoded_len = htobe32(len);
    std::copy_n(reinterpret_cast<const char*>(&encoded_len), 4, begin);

    auto rb = std::make_shared<std::string>();
    using wrap_handler_type =
        wrap_handler<Response, Transform,
                     typename boost::asio::handler_type<
                         Handler, void(transform_result)>::type>;
    // TODO: use references for request and response buffers
    zk_op<decltype(sock_),
          typename boost::asio::handler_type<
              wrap_handler_type, void(boost::system::error_code)>::type>
        zop{wrap_handler_type{*this, rb, std::forward<Transform>(transform),
                              init.completion_handler},
            sock_, oc, std::move(b), std::move(rb)};

   defer([zop = std::move(zop)](auto self) mutable {
      if (!self->connected() || self->op_running_) {
        self->q_.emplace([zop = std::move(zop)]() mutable {
          zop(boost::system::error_code{});
        });
      } else {
        self->op_running_ = true;
        zop(boost::system::error_code{});
      }
    });

    return init.result.get();
  }

  void do_operations() {
    defer([](auto self) {
      if (self->connected() && !self->q_.empty()) {
        auto op = std::move(self->q_.front());
        // pop _before_ exec in case op initiates further operations of its own
        self->q_.pop();
        op();
      }
    });
  }

  void start_connection() {
    defer([](auto self) {
      auto&& addr = self->addrs_.get();
      const boost::asio::ip::tcp::endpoint ep{
          boost::asio::ip::address::from_string(addr.host), addr.port};

      self->resolver_.async_resolve(ep,
                                    self->strand_.wrap([self](auto ec, auto i) {
                                      if (ec) {
                                        trace_error("resolve", ec);
                                        self->start_connection();
                                        return;
                                      }

                                      self->connect(i);
                                    }));
    });
  }

  void connect(boost::asio::ip::tcp::resolver::iterator i) {
    // TODO: send to op queue as well
    boost::asio::async_connect(
        sock_, i, strand_.wrap([self = shared_from_this()](auto ec, auto i) {
          if (ec) {
            trace_error("connect", ec);
            if (i == boost::asio::ip::tcp::resolver::iterator()) {
              // TODO: wait a bit; then restart connection to a different serv
              self->start_connection();
            } else {
              self->connect(++i);
            }
            return;
          }

          self->state_ = state::connecting;
          self->start_session();
        }));
  }

  bool connected() const {
    assert(strand_.running_in_this_thread());
    return state_ == state::connected;
  }

  void reconnect() {
    // TODO: let listeners know
    assert(strand_.running_in_this_thread());
    state_ = state::connecting;
    start_connection();
  }

  void start_session() {
    connect_request req;
    owned_buffer wb;
    req.serialize(wb);
    auto buf = std::make_shared<owned_buffer>(make_zpayload(wb));

    boost::asio::async_write(
        sock_, boost::asio::buffer(buf->data(), buf->size()),
        strand_.wrap([self = shared_from_this(), buf](auto ec, auto) {
          if (ec) {
            trace_error("connect request", ec);
            // reconnect | abort (in case of auth failures)
            return;
          }

          auto rb = std::make_shared<std::string>();
          zk_read(self->sock_, rb, self->strand_.wrap([self, rb](auto ec) {
            if (ec) {
              trace_error("connect response", ec);
              return;
            }
            auto resp = connect_response::deserialize(
                boost::asio::const_buffer(rb->data(), rb->size()));
            detail::trace(resp);  //  debug
            self->session_state_.emplace(
                session_state{resp.protocol_version, duration(resp.timeout),
                              resp.session_id, resp.read_only});

            self->state_ = state::connected;
            self->do_operations();
            self->start_ping();
          }));
        }));
  }

  void close() {
    // TODO
  }

  void start_ping() {
    defer([](auto self) {
      if (!self->connected()) {
        return;
      }

      assert(self->session_state_);
      auto period_ms =
          static_cast<long>(self->session_state_->timeout.count() * 0.75);
      if (period_ms < 10) period_ms = 10;

      self->timer_.expires_from_now(duration(period_ms));
      self->timer_.async_wait([self](auto ec) {
        if (ec) {  // canceled; shutting down connection
          return;
        }
        const auto& ping = ping_request::make();
        boost::asio::async_write(
            self->sock_, boost::asio::buffer(ping.b.data(), ping.b.size()),
            self->strand_.wrap([self](auto ec, auto) {
              if (ec) {
                trace_error("ping", ec);
                self->reconnect();
                return;
              }
              // TODO: async_read ping acknowledgement; handle failures accordingly
              // Also, use zk_op to do this; to synchronize txns

              self->start_ping();
            }));
      });
    });
  }

  void disconnected() {
    assert(strand_.running_in_this_thread());
    state_ = state::session_expired;
    session_state_ = boost::none;
  }
};

}  // end namespace zookeeper
