#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class RunGCRequest : public Request
{
public:
  RunGCRequest(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics
  )
    : Request(service, cq_, metrics)
  {
  };

  virtual ~RunGCRequest() {
  }

};

} /* request */
} /* api */
} /* mhconfig */

#endif
