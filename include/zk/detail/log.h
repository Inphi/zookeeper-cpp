#pragma once

#include <boost/asio/error.hpp>
#include <boost/utility/string_ref.hpp>
#include <cinttypes>
#include <iostream>
#include <system_error>
#include "zk/proto.h"

namespace zookeeper {
namespace detail {
void trace(boost::string_ref msg) {
#ifdef ZK_LOGGING
  std::cerr << msg << '\n';
#endif
}

void trace_error(boost::string_ref msg, boost::system::error_code ec) {
#ifdef ZK_LOGGING
  std::cerr << msg << ": " << ec.message() << '\n';
#endif
}

void trace(const connect_response& response) {
#ifdef ZK_LOGGING
  std::cerr << "ver: " << response.protocol_version << '\n';
  std::cerr << "timeout: " << response.timeout << '\n';
  fprintf(stderr, "session_id: 0x%" PRIx64 "\n", response.session_id);
  std::cerr << "read_only: " << response.read_only << '\n';
#endif
}

void trace(const reply_header& rh) {
#ifdef ZK_LOGGING
  std::cout << "xid: " << rh.xid << std::endl;
  std::cout << "zxid: " << rh.zxid << std::endl;
  std::cout << "err: " << rh.err << std::endl;
#endif
}

}  // end namespace detail
}  // end namespace zookeeper
