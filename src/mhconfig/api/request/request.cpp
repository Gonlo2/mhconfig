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

void Request::on_proceed(
  uint8_t status,
  CustomService* service,
  grpc::ServerCompletionQueue* cq,
  SchedulerQueue::Sender* scheduler_sender,
  metrics::MetricsService& metrics,
  uint_fast32_t& sequential_id
) {
  switch (static_cast<RequestStatus>(status)) {
    case RequestStatus::CREATE: {
      metricate_ = (sequential_id & 0xff) == 0;
      sequential_id = (sequential_id+1) & 0xefffffff;
      if (metricate_) {
        start_time_ = jmutils::monotonic_now();
      }

      clone_and_subscribe(service, cq);

      request(scheduler_sender);
      break;
    }
    case RequestStatus::PROCESS:
      if (metricate_) {
        auto end_time = jmutils::monotonic_now();

        double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end_time - start_time_
        ).count();

        metrics.add(
          metrics::MetricsService::MetricId::API_DURATION_NANOSECONDS,
          {{"type", name()}},
          duration_ns
        );
      }
      break;
    case RequestStatus::FINISH:
      break;
  }
}

bool Request::reply() {
  finish();
  return true;
}

} /* request */
} /* api */
} /* mhconfig */
