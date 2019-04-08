#pragma once

#include <boost/asio/buffer.hpp>
#include <boost/endian/conversion.hpp>
#include <cassert>
#include "zk/owned_buffer.h"

namespace zookeeper {

enum class op_code : int32_t {
  auth = 100,
  create = 1,
  remove = 2,  // delete
  exists = 3,
  get_data = 4,
  set_data = 5,
  get_acl = 6,
  set_acl = 7,
  get_children = 8,
  sync = 9,
  ping = 11,
  close = -11,

  // new zookeeper operations
  multi = 14,
  remove_watches = 16
};

enum class create_mode : std::int32_t {
  persistent = 0,
  ephemeral = 1,
  persistent_sequential = 2,
  ephemeral_sequential = 3,
};

auto make_zpayload(const owned_buffer& buf) {
  owned_buffer newbuf{buf.size() + 4};

  const int32_t siz =
      static_cast<int32_t>(boost::endian::native_to_big(buf.size()));
  newbuf.write(boost::asio::buffer(reinterpret_cast<const char*>(&siz), 4));
  newbuf.write(boost::asio::buffer(buf.data(), buf.size()));
  return newbuf;
}

owned_buffer read_zkbuffer(const boost::asio::const_buffer& buf) {
  if (boost::asio::buffer_size(buf) < 4)
    throw "invalid connect response buffer";

  int32_t encoded_len = *boost::asio::buffer_cast<const int32_t*>(buf);
  const auto len = boost::endian::big_to_native(encoded_len);
  if (boost::asio::buffer_size(buf + 4) < static_cast<std::size_t>(len))
    throw "invalid connect response buffer";

  owned_buffer b{static_cast<std::size_t>(len)};
  b.write(boost::asio::const_buffer(
      boost::asio::buffer_cast<const void*>(buf + 4), len));
  return b;
}

// a length prefixed-buffer
std::string deserialize_buffer(boost::asio::const_buffer& buf) {
  if (boost::asio::buffer_size(buf) < 4) {
    throw std::runtime_error("insufficient buffer length for length desc");
  }
  int32_t len = *boost::asio::buffer_cast<const int32_t*>(buf);
  buf += 4;
  len = boost::endian::big_to_native(len);
  if (boost::asio::buffer_size(buf) < static_cast<uint32_t>(len)) {
    throw std::runtime_error("insufficient buffer length for body");
  }

  std::string out(static_cast<const char*>(buf.data()),
                  static_cast<const char*>(buf.data()) + len);
  buf += len;
  return out;
}

// Requires: MoveConstructible<T> && Deserializable<T>
template <typename T>
std::vector<T> deserialize_vector(boost::asio::const_buffer& buf) {
  std::vector<T> vec;

  if (buf.size() < 4) {
    throw std::runtime_error("invalid acl_response");
  }

  int32_t len = *boost::asio::buffer_cast<const int32_t*>(buf);
  buf += 4;
  len = boost::endian::big_to_native(len);
  for (int i = 0; i < len; ++i) {
    T t = T::deserialize(buf);
    vec.emplace_back(std::move(t));
  }

  return vec;
}

template <>
std::vector<char> deserialize_vector(boost::asio::const_buffer& buf) {
  std::vector<char> vec;

  if (buf.size() < 4) {
    throw std::runtime_error("invalid acl_response");
  }

  int32_t len = *boost::asio::buffer_cast<const int32_t*>(buf);
  buf += 4;
  len = boost::endian::big_to_native(len);
  vec.resize(len);
  std::copy_n(static_cast<const char*>(buf.data()), len, &vec[0]);

  return vec;
}

template <>
std::vector<std::string> deserialize_vector(boost::asio::const_buffer& buf) {
  std::vector<std::string> vec;

  if (buf.size() < 4) {
    throw std::runtime_error("invalid acl_response");
  }

  int32_t len = *boost::asio::buffer_cast<const int32_t*>(buf);
  buf += 4;
  len = boost::endian::big_to_native(len);
  for (int i = 0; i < len; ++i) {
    auto t = deserialize_buffer(buf);
    vec.emplace_back(std::move(t));
  }

  return vec;
}

void serialize(const int32_t& i, owned_buffer& b) {
  const auto data = boost::endian::native_to_big(i);
  b.write(boost::asio::buffer(reinterpret_cast<const char*>(&data), 4));
}

void serialize(const std::string& str, owned_buffer& b) {
  const int32_t siz =
      static_cast<int32_t>(boost::endian::native_to_big(str.size()));
  b.write(boost::asio::buffer(reinterpret_cast<const char*>(&siz), 4));
  b.write(boost::asio::buffer(str.data(), str.size()));
}

// Requires: T is Serializable
template <typename T>
void serialize(const std::vector<T>& in, owned_buffer& b) {
  const int32_t siz =
      static_cast<int32_t>(boost::endian::native_to_big(in.size()));
  b.write(boost::asio::buffer(reinterpret_cast<const char*>(&siz), 4));
  for (const auto& i : in) {
    i.serialize(b);
  }
}

template <>
void serialize(const std::vector<char>& in, owned_buffer& b) {
  const int32_t siz =
      static_cast<int32_t>(boost::endian::native_to_big(in.size()));
  b.write(boost::asio::buffer(reinterpret_cast<const char*>(&siz), 4));
  b.write(boost::asio::buffer(&in[0], in.size()));
}

std::size_t zsize(const std::string& s) { return 4 + s.size(); }

std::size_t zsize(const std::vector<char>& s) { return 4 + s.size(); }

struct stat {
  int64_t czxid;
  int64_t mzxid;
  int64_t ctime;
  int64_t mtime;
  int32_t version;
  int32_t cversion;
  int32_t aversion;
  int64_t ephemeral_owner;
  int32_t data_length;
  int32_t num_children;
  int64_t pzxid;

  static stat deserialize(boost::asio::const_buffer& buf) {
    stat st;
    st.czxid = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    st.mzxid = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    st.ctime = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    st.mtime = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    st.version = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 4;
    st.cversion = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 4;
    st.aversion = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 4;
    st.ephemeral_owner = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    st.data_length = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 4;
    st.num_children = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 4;
    st.pzxid = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    return st;
  }
};

struct acl {
  enum class permission : int32_t {
    none = 0,
    read = 0b00001,
    write = 0b00010,
    create = 0b00100,
    _delete = 0b01000,
    all = 0b11111,
  };

  permission perms = permission::none;
  std::string scheme;
  std::string id;

  static std::vector<acl> open_unsafe() {
    return {{permission::all, "world", "anyone"}};
  }

  static std::vector<acl> creator_all() {
    return {{permission::all, "auth", ""}};
  }

  static std::vector<acl> read_unsafe() {
    return {{permission::read, "world", "anyone"}};
  }

  void serialize(owned_buffer& b) const {
    const auto encoded_perms =
        boost::endian::native_to_big(static_cast<int32_t>(perms));
    b.write(
        boost::asio::buffer(reinterpret_cast<const char*>(&encoded_perms), 4));

    ::zookeeper::serialize(scheme, b);
    ::zookeeper::serialize(id, b);
  }

  static acl deserialize(boost::asio::const_buffer buf) {
    acl a;
    int32_t data = *boost::asio::buffer_cast<const int32_t*>(buf);
    a.perms = static_cast<permission>(boost::endian::big_to_native(data));
    buf += 4;
    a.scheme = deserialize_buffer(buf);
    a.id = deserialize_buffer(buf);
    return a;
  }
};

struct request_header {
  int32_t xid;
  int32_t opcode;

  void serialize(owned_buffer& b) const {
    b.grow(sizeof(int32_t) + sizeof(int32_t));

    int32_t _xid = boost::endian::native_to_big(xid);
    int32_t _opcode = boost::endian::native_to_big(opcode);
    b.write(boost::asio::buffer(reinterpret_cast<char*>(&_xid), 4));
    b.write(boost::asio::buffer(reinterpret_cast<char*>(&_opcode), 4));
  }

  int32_t size() const { return 8; }
};

struct create_request {
  std::string path;
  std::vector<char> data;
  std::vector<acl> acls;
  int32_t flags = 0;

  void serialize(owned_buffer& b) const {
    b.grow(size());

    ::zookeeper::serialize(path, b);

    ::zookeeper::serialize(data, b);

    // hack for now
    ::zookeeper::serialize(acls, b);

    int32_t flags_be = boost::endian::native_to_big(flags);
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&flags_be), 4));
  }

  int32_t size() const {
    return static_cast<int32_t>(zsize(path) + zsize(data) +
                                4 /* hack for acls */ + 4);
  }
};

struct create_response {
  std::string path;

  static create_response deserialize(boost::asio::const_buffer& buf) {
    create_response c;
    c.path = deserialize_buffer(buf);
    return c;
  }
};

struct delete_request {
  std::string path;
  int32_t version;

  void serialize(owned_buffer& b) const {
    ::zookeeper::serialize(path, b);
    ::zookeeper::serialize(version, b);
  }
};

struct path_watch_request {
  std::string path;
  bool watch{false};

  void serialize(owned_buffer& b) const {
    ::zookeeper::serialize(path, b);
    const uint8_t watch_i = watch;
    b.write(boost::asio::buffer(&watch_i, 1));
  }
};

struct stat_response {
  stat st;

  static stat_response deserialize(boost::asio::const_buffer& buf) {
    stat_response response;
    response.st = stat::deserialize(buf);
    return response;
  }
};

using exists_request = path_watch_request;
using exists_response = stat_response;

using get_children_request = path_watch_request;

struct get_children_response {
  std::vector<std::string> children;

  static auto deserialize(boost::asio::const_buffer& buf) {
    get_children_response response;
    response.children = deserialize_vector<std::string>(buf);
    return response;
  }
};

using get_data_request = path_watch_request;

struct get_data_response {
  std::vector<char> data;
  stat st;

  // TODO: make stat::deserialize free standing
  static auto deserialize(boost::asio::const_buffer& buf) {
    get_data_response response;
    response.data = deserialize_vector<char>(buf);
    response.st = stat::deserialize(buf);
    return response;
  }
};

struct set_data_request {
  std::string path;
  std::vector<char> data;
  std::int32_t version;

  void serialize(owned_buffer& b) const {
    ::zookeeper::serialize(path, b);
    ::zookeeper::serialize(data, b);
    ::zookeeper::serialize(version, b);
  }
};

using set_data_response = stat_response;

struct get_acl_request {
  std::string path;

  void serialize(owned_buffer& b) const { ::zookeeper::serialize(path, b); }
};

struct set_acl_request {
  std::string path;
  std::vector<acl> acls;
  std::int32_t version;

  void serialize(owned_buffer& b) const {
    ::zookeeper::serialize(path, b);
    ::zookeeper::serialize(acls, b);
    ::zookeeper::serialize(version, b);
  }
};

using set_acl_response = stat_response;

struct get_acl_response {
  std::vector<acl> acls;
  stat st;

  static get_acl_response deserialize(boost::asio::const_buffer& buf) {
    get_acl_response response;
    response.acls = deserialize_vector<acl>(buf);
    response.st = stat::deserialize(buf);

    return response;
  }
};

template <typename Request>
struct request_payload {
  request_header hdr;
  Request t;

  void serialize(owned_buffer& b) const {
    hdr.serialize(b);
    t.serialize(b);
  }
};

// create opcode == 1
template <typename T>
auto make_payload(op_code opcode, T&& t) {
  owned_buffer b;

  char* begin = b.data() + b.size();
  // TODO: owned_buffer::skip
  int32_t len = 0;  // placeholder
  b.write(boost::asio::buffer(reinterpret_cast<const char*>(&len), 4));

  request_header hdr;
  hdr.xid = -1;
  hdr.opcode = static_cast<int32_t>(opcode);

  request_payload<T> payload{hdr, std::forward<T>(t)};
  payload.serialize(b);

  len = b.data() + b.size() - begin - 4;
  const auto encoded_len = boost::endian::native_to_big(len);
  std::copy_n(reinterpret_cast<const char*>(&encoded_len), 4, begin);
  return b;
}

struct connect_request {
  int32_t protocol_version = 0;
  int64_t last_zxid = 0;
  int32_t timeout = 4000;  // for now
  int64_t session_id = 0;
  owned_buffer passwd;
  bool read_only = false;

  connect_request() {
    const char pass[] = {
        '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
        '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0',
    };
    passwd.write(boost::asio::buffer(pass, 16));
  }

  void serialize(owned_buffer& b) const {
    const auto siz = sizeof(int32_t) + sizeof(int64_t) + sizeof(int32_t) +
                     sizeof(int64_t) + passwd.size() + 1;

    const auto pv = boost::endian::native_to_big(protocol_version);
    const auto lz = boost::endian::native_to_big(last_zxid);
    const auto t = boost::endian::native_to_big(timeout);
    const auto sid = boost::endian::native_to_big(session_id);
    const uint8_t read_only_s = read_only;

    const auto passwdlen = boost::endian::native_to_big(passwd.size());

    b.grow(siz);
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&pv), 4));
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&lz), 8));
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&t), 4));
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&sid), 8));
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&passwdlen), 4));
    b.write(boost::asio::buffer(passwd.data(), passwd.size()));
    b.write(boost::asio::buffer(&read_only_s, 1));
  }
};

struct connect_response {
  int32_t protocol_version;
  int32_t timeout;
  int64_t session_id;
  owned_buffer passwd;
  bool read_only;

  static connect_response deserialize(const boost::asio::const_buffer& buf) {
    if (boost::asio::buffer_size(buf) < 16)
      throw "invalid connect response buffer";

    int32_t protocol_version = *boost::asio::buffer_cast<const int32_t*>(buf);
    int32_t timeout = *boost::asio::buffer_cast<const int32_t*>(buf + 4);
    int64_t session_id = *boost::asio::buffer_cast<const int64_t*>(buf + 8);
    auto passwd = read_zkbuffer(buf + 16);
    assert(passwd.size() == 16);

    connect_response c;
    c.protocol_version = boost::endian::big_to_native(protocol_version);
    c.timeout = boost::endian::big_to_native(timeout);
    c.session_id = boost::endian::big_to_native(session_id);
    c.passwd = std::move(passwd);
    return c;
  }
};

struct empty_response {
  static empty_response deserialize(const boost::asio::const_buffer& buf) {
    return {};
  }
};

struct reply_header {
  int32_t xid;
  int64_t zxid;
  int32_t err;

  static reply_header deserialize(boost::asio::const_buffer& buf) {
    const auto siz = boost::asio::buffer_size(buf);
    if (siz < 16) throw "invalid reply header";

    int32_t xid = *boost::asio::buffer_cast<const int32_t*>(buf);
    buf += 4;
    int32_t zxid = *boost::asio::buffer_cast<const int64_t*>(buf);
    buf += 8;
    int32_t err = *boost::asio::buffer_cast<const int32_t*>(buf);
    buf += 4;

    reply_header rh;
    rh.xid = boost::endian::big_to_native(xid);
    rh.zxid = boost::endian::big_to_native(zxid);
    rh.err = boost::endian::big_to_native(err);
    return rh;
  }

  static constexpr int32_t size() { return 16; }
};

struct ping_request {
  owned_buffer b;

  static const int32_t ping_xid = -2;

  ping_request() {
    request_header hdr;
    hdr.xid = ping_xid;
    hdr.opcode = static_cast<int32_t>(op_code::ping);

    char* begin = b.data() + b.size();
    int32_t len = 0;  // placeholder
    b.write(boost::asio::buffer(reinterpret_cast<const char*>(&len), 4));

    hdr.serialize(b);
    len = static_cast<int32_t>(b.data() + b.size() - begin - 4);
    const auto encoded_len = boost::endian::native_to_big(len);
    std::copy_n(reinterpret_cast<const char*>(&encoded_len), 4, begin);
  }

  static const ping_request& make() {
    static ping_request p;
    return p;
  }
};

}  // end namespace zookeeper
