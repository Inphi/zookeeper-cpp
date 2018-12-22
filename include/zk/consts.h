#pragma once

#include <cstdint>

namespace zookeeper {

enum class event_type : std::int32_t {
  none = -1,
  node_created = 1,
  node_deleted = 2,
  node_data_changed = 3,
  node_children_changed = 4,
  data_watch_removed = 5,
  child_watch_removed = 6,
};

enum class keeper_state : std::int32_t {
  disconnected = 0,
  sync_connected = 3,
  auth_failed = 4,
  connected_read_only = 5,
  expired = -112,
};

}  // end namespace zookeeper
