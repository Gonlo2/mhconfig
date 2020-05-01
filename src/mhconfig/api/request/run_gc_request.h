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

namespace run_gc {
  enum Type {
    CACHE_GENERATION_0 = 0,
    CACHE_GENERATION_1 = 1,
    CACHE_GENERATION_2 = 2,
    DEAD_POINTERS = 3,
    NAMESPACES = 4,
    VERSIONS = 5
  };
}


class RunGCRequest : public Request
{
public:
  RunGCRequest(
    CustomService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics
  )
    : Request(service, cq_, metrics)
  {
  };

  virtual ~RunGCRequest() {
  }

  virtual run_gc::Type type() = 0;
  virtual uint32_t max_live_in_seconds() = 0;

};

} /* request */
} /* api */
} /* mhconfig */

#endif
