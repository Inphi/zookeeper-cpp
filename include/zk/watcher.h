#pragma once

#include <string>
#include "zk/consts.h"

namespace zookeeper {

struct watch_event {
  event_type type;
  keeper_state state;
  std::string path;
};

class watcher {
 public:
  virtual ~watcher() = default;
  virtual void process(watch_event e) = 0;
};

}  // end namespace zookeeper
