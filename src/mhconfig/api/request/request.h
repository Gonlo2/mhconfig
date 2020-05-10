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
  Request(
      CustomService* service,
      grpc::ServerCompletionQueue* cq,
      metrics::MetricsService& metrics
  );
  virtual ~Request();

  std::shared_ptr<Session> proceed() override;

  bool reply();

protected:
  metrics::MetricsService& metrics_;

  virtual void request() = 0;
  virtual void finish() = 0;

private:
  enum Status {
    CREATE,
    PROCESS,
    FINISH
  };

  Status status_{Status::CREATE};
  jmutils::time::MonotonicTimePoint start_time_;

  const std::string status();

};

} /* request */
} /* api */
} /* mhconfig */

#endif
