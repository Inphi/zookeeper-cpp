#include <thread>
#include <boost/asio/io_service.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/spawn.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <functional>
#include "zk/context.h"

int main(int ac, char** av) {
  namespace asio = boost::asio;

  if (ac < 2) {
    std::cerr << "missing port. usage : " << av[0] << " <port>\n";
    std::exit(1);
  }

  const auto port = static_cast<unsigned short>(std::atoi(av[1]));

  asio::io_service ios;
  auto ctx = zookeeper::context::make(
      ios, std::string("0.0.0.0:") + std::to_string(port),
      zookeeper::duration(4000));

  // callback style
  ctx->create("/bloody_roar", {}, zookeeper::acl::open_unsafe(),
              zookeeper::create_mode::persistent, [](auto result) {
                if (result.is_ok()) {
                  std::cerr << "path: " << result.ok() << std::endl;
                } else {
                  std::cerr << "failure: " << result.err().message()
                            << std::endl;
                }
              });

  // future style
  auto future =
      ctx->create("/bloody_roar", {}, zookeeper::acl::open_unsafe(),
                  zookeeper::create_mode::persistent, asio::use_future);
  // make sure we don't block this io service thread
  std::thread([f = std::move(future)]() mutable {
    auto result = f.get();
    if (result.is_err()) {
      std::cerr << "status: " << result.err() << std::endl;
    } else {
      std::cerr << "status: success\n";
    }
  })
      .detach();

  // fancy coroutine style!
  asio::spawn(ios, [ctx](auto yield) {
    auto result = ctx->create("/bloody_roar", {}, zookeeper::acl::open_unsafe(),
                              zookeeper::create_mode::persistent, yield);
    if (result.is_err())
      std::cerr << "create status: " << result.err() << std::endl;
    else
      std::cerr << "create success\n";

    auto cresult = ctx->get_children("/", false, yield);
    if (!cresult.is_ok()) {
      std::cerr << "err: " << cresult.err().message() << std::endl;
      } else {
      std::cerr << "get children success\n";
    }
    if (cresult.is_ok()) {
      for (const auto& child : cresult.ok()) {
        std::cerr << child << " ";
      }
      std::cerr << "\n";
    }

    auto eresult = ctx->exists("/non_existent_path", false, yield);
    if (eresult.is_err())
      std::cerr << "exists result: " << eresult.err().message() << std::endl;
    else
      std::cerr << "path exists\n";

    auto data_result =  ctx->get_data("/foo", false, yield);
    if (data_result.is_ok()) {
      auto& data = data_result.ok().first;
      const std::string str{data.begin(), data.end()};
      std::cout << "foo: " << str << std::endl;
    }

    auto remove_result = ctx->remove("/non_existent_path", boost::none, yield);
    if (remove_result.is_err())
      std::cerr << "remove: " << remove_result.err() << std::endl;
    else
      std::cerr << "remove success\n";

    auto bad_result =
        ctx->create("invalid_path", {}, zookeeper::acl::open_unsafe(),
                    zookeeper::create_mode::persistent, yield);
    std::cerr << bad_result.err().message() << std::endl;
    assert(bad_result.err() == zookeeper::error::bad_arguments);
  });

  
  auto g = asio::executor_work_guard<asio::io_context::executor_type>{
      ios.get_executor()};

  ios.run();

  return 0;
}
