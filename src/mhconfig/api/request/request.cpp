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
  context_t* ctx,
  uint_fast32_t& sequential_id
) {
  switch (static_cast<RequestStatus>(status)) {
    case RequestStatus::CREATE: {
      metricate_ = (sequential_id++ & 0xff) == 0;
      if (metricate_) {
        start_time_ = jmutils::monotonic_now();
      }

      clone_and_subscribe(service, cq);

      request(ctx);
      break;
    }
    case RequestStatus::PROCESS:
      if (metricate_) {
        auto end_time = jmutils::monotonic_now();

        double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          end_time - start_time_
        ).count();

        ctx->metrics.add(
          Metrics::Id::API_DURATION_NANOSECONDS,
          {{"type", name()}},
          duration_ns
        );
      }
      break;
    case RequestStatus::FINISH:
      break;
  }
}

} /* request */
} /* api */
} /* mhconfig */
