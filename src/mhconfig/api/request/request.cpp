#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace api
{
namespace request
{

Request::Request(
  CustomService* service,
  grpc::ServerCompletionQueue* cq,
  metrics::MetricsService& metrics
)
  : Session(service, cq),
    metrics_(metrics)
{
}

Request::~Request() {
}

std::shared_ptr<Session> Request::proceed() {
  std::lock_guard<std::recursive_mutex> mlock(mutex_);
  if (is_destroyed()) {
    spdlog::debug("Isn't possible call to proceed in a destroyed request");
  } else {
    spdlog::debug("Received gRPC event {} in {} status", name(), status());

    if (status_ == Status::CREATE) {
      status_ = Status::PROCESS;

      auto new_request = clone();
      new_request->subscribe();

      if ((((uintptr_t)tag()) & 0xfff) == 0) {
        start_time_ = jmutils::time::monotonic_now();
      }
      request();
    } else if (status_ == Status::FINISH) {
      if ((((uintptr_t)tag()) & 0xfff) == 0) {
        auto end_time = jmutils::time::monotonic_now();

        double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end_time - start_time_
        ).count();

        metrics_.observe(
          metrics::MetricsService::MetricId::API_DURATION_NANOSECONDS,
          {{"type", name()}},
          duration_ns
        );
      }

      return destroy();
    } else {
      assert(false);
    }
  }
  return nullptr;
}

bool Request::reply() {
  std::lock_guard<std::recursive_mutex> mlock(mutex_);
  if (is_destroyed()) {
    spdlog::debug("Isn't possible call to reply in a destroyed request");
    return false;
  }

  assert(status_ == Status::PROCESS);
  status_ = Status::FINISH;

  // TODO try to move the logic here
  // The call to enqueue the FINISH status need be the
  // last call because the api workers could delete
  // the pointer after it
  finish();
  return true;
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
