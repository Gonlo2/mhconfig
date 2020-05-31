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

  std::shared_ptr<Session> proceed(
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics,
    uint_fast32_t& sequential_id
  ) override;

  bool reply();

protected:
  virtual void request(
    SchedulerQueue::Sender* scheduler_sender
  ) = 0;
  virtual void finish() = 0;

private:
  enum RequestStatus {
    CREATE,
    PROCESS,
    FINISH
  };

  jmutils::time::MonotonicTimePoint start_time_;
  RequestStatus status_{RequestStatus::CREATE};
  bool metricate_;

  const std::string status();

};

} /* request */
} /* api */
} /* mhconfig */

#endif
