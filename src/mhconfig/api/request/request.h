#ifndef MHCONFIG__API__REQUEST__REQUEST_H
#define MHCONFIG__API__REQUEST__REQUEST_H

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "jmutils/time.h"
#include "mhconfig/api/session.h"
#include "mhconfig/metrics.h"
#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class Request : public Session
{
public:
  template <typename T>
  Request(
    T&& ctx
  ) : Session(std::forward<T>(ctx)) {
  }

  virtual ~Request() {
  }

protected:
  void on_write() override {
  }
};

} /* request */
} /* api */
} /* mhconfig */

#endif
