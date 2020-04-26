#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/worker/common.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace run_gc_request
{

using jmutils::container::Queue;
using namespace mhconfig::worker;

class RunGCRequest : public Request
{
public:
  RunGCRequest(
      mhconfig::proto::MHConfig::AsyncService* service,
      grpc::ServerCompletionQueue* cq_,
      Metrics& metrics,
      Queue<command::command_t>& scheduler_queue
  );
  virtual ~RunGCRequest();

  const std::string name() const override;
  const uint32_t id() const override;

  Request* clone() override;
  void subscribe() override;

protected:
  grpc::ServerAsyncResponseWriter<mhconfig::proto::RunGCResponse> responder_;
  Queue<command::command_t>& scheduler_queue_;

  mhconfig::proto::RunGCRequest request_;
  mhconfig::proto::RunGCResponse response_;

  void notify_scheduler_if_possible();

  void request() override;
  void finish() override;
};

} /* get_request */
} /* request */
} /* api */
} /* mhconfig */

#endif
