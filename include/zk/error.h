#pragma once

#include <system_error>

namespace zookeeper {

enum class error : std::int32_t {
  connection_loss = -4,
  unimplemented = -6,
  // operation_timeout = -7,
  bad_arguments = -8,

  api_error = -100,
  no_node = -101,
  no_auth = -102,
  bad_version = -103,
  node_exists = -110,
  not_empty = -111,
  session_expired = -112,
  invalid_acl = -113,
  auth_failed = -115,
  closing = -116,
  session_moved = -118,
  not_readonly = -119,
  // timeout
};

class zk_error_category : public ::std::error_category {
public:
  const char* name() const noexcept override {
    return "zookeeper";
  }

  std::string message(int ev) const override {
    switch (static_cast<error>(ev)) {
      case error::connection_loss:
        return "connection loss";
      case error::unimplemented:
        return "unimplemented";
      case error::bad_arguments:
        return "bad arguments";
      case error::api_error:
        return "api error";
      case error::no_node:
        return "no node";
      case error::no_auth:
        return "no auth";
      case error::bad_version:
        return "bad version";
      case error::node_exists:
        return "node exists";
      case error::not_empty:
        return "not empty";
      case error::session_expired:
        return "session expired";
      case error::invalid_acl:
        return "invalid acl";
      case error::auth_failed:
        return "auth failed";
      case error::closing:
        return "closing";
      case error::session_moved:
        return "session moved";
      case error::not_readonly:
        return "not readonly";
     default:
        return "zookeeper error";
    }
  }

  ::std::error_condition default_error_condition(int ev) const
      noexcept override {
    return ::std::error_condition{ev, *this};
    }
};

const zk_error_category _zk_ec{};

std::error_code make_error_code(error e) {
  return {static_cast<int>(e), _zk_ec};
}

} // end namespace zookeeper

namespace std {
template <>
struct is_error_code_enum<zookeeper::error> : std::true_type {};
}  // end namespace std
