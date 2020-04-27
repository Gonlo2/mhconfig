#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace api
{
namespace request
{

  Request::Request(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq,
    Metrics& metrics
  ) :
      service_(service),
      cq_(cq),
      metrics_(metrics)
  {
  }

  Request::~Request() {
  }

  void Request::proceed() {
    spdlog::debug("Received gRPC event {} in {} status", name(), status());

    if (status_ == Status::CREATE) {
      status_ = Status::PROCESS;

      auto new_request = clone();
      new_request->subscribe();

      start_time_ = std::chrono::high_resolution_clock::now();
      request();
    } else if (status_ == Status::FINISH) {
      auto end_time = std::chrono::high_resolution_clock::now();

      double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_time - start_time_
      ).count();

      metrics_.api_duration(name(), duration_ns);

      delete this;
    } else {
      assert(false);
    }
  }

  void Request::reply() {
    assert(status_ == Status::PROCESS);
    status_ = Status::FINISH;
    finish();
  }

  const std::string Request::status() {
    switch (status_) {
      case Status::CREATE: return "CREATE";
      case Status::PROCESS: return "PROCESS";
      case Status::FINISH: return "FINISH";
    }

    return "unknown";
  }

} /* request */
} /* api */
} /* mhconfig */
