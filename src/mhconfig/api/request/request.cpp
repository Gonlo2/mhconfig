#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace api
{
namespace request
{

Request::Request() {
}

Request::~Request() {
}

std::shared_ptr<Session> Request::proceed(
  CustomService* service,
  grpc::ServerCompletionQueue* cq,
  SchedulerQueue::Sender* scheduler_sender,
  metrics::MetricsService& metrics,
  uint_fast32_t& sequential_id
) {
  std::lock_guard<std::recursive_mutex> mlock(mutex_);
  if (is_destroyed()) {
    spdlog::debug("Isn't possible call to proceed in a destroyed request");
  } else {
    spdlog::debug("Received gRPC event {} in {} status", name(), status());

    if (status_ == RequestStatus::CREATE) {
      status_ = RequestStatus::PROCESS;

      metricate_ = (sequential_id & 0xff) == 0;
      sequential_id = (sequential_id+1) & 0xefffffff;
      if (metricate_) {
        start_time_ = jmutils::time::monotonic_now();
      }

      clone_and_subscribe(service, cq);

      request(scheduler_sender);
    } else if (status_ == RequestStatus::FINISH) {
      if (metricate_) {
        auto end_time = jmutils::time::monotonic_now();

        double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end_time - start_time_
        ).count();

        metrics.observe(
          metrics::MetricsService::ObservableId::API_DURATION_NANOSECONDS,
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

  assert(status_ == RequestStatus::PROCESS);
  status_ = RequestStatus::FINISH;

  // TODO try to move the logic here
  // The call to enqueue the FINISH status need be the
  // last call because the api workers could delete
  // the pointer after it
  finish();
  return true;
}

const std::string Request::status() {
  switch (status_) {
    case RequestStatus::CREATE: return "CREATE";
    case RequestStatus::PROCESS: return "PROCESS";
    case RequestStatus::FINISH: return "FINISH";
  }

  return "unknown";
}

} /* request */
} /* api */
} /* mhconfig */
