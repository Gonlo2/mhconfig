#ifndef MHCONFIG__API__REQUEST__REQUEST_H
#define MHCONFIG__API__REQUEST__REQUEST_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "mhconfig/api/session.h"
#include "mhconfig/metrics/metrics_service.h"
#include "jmutils/time.h"

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
  Request();
  virtual ~Request();

  void on_proceed(
    uint8_t status,
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    auth::Acl* acl,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService* metrics,
    uint_fast32_t& sequential_id
  ) override;

protected:
  enum class RequestStatus {
    CREATE = 0,
    FINISH = 1,
    PROCESS = 2
  };

  virtual void request(
    auth::Acl* acl,
    SchedulerQueue::Sender* scheduler_sender
  ) = 0;

  inline void* tag(RequestStatus status) {
    return raw_tag(static_cast<uint8_t>(status));
  }

private:
  jmutils::MonotonicTimePoint start_time_;
  bool metricate_;

  const std::string status();

};

} /* request */
} /* api */
} /* mhconfig */

#endif
