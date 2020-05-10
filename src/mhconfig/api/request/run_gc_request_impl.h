#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H

#include "jmutils/container/queue.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/run_gc_command.h"
#include "mhconfig/api/request/run_gc_request.h"
#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace api
{
namespace request
{

using jmutils::container::Queue;
using namespace mhconfig::scheduler::command;

class RunGCRequestImpl : public Request, public RunGCRequest, public std::enable_shared_from_this<RunGCRequestImpl>
{
public:
  RunGCRequestImpl(
      CustomService* service,
      grpc::ServerCompletionQueue* cq_,
      metrics::MetricsService& metrics,
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~RunGCRequestImpl();

  const std::string name() const override;

  std::shared_ptr<Session> clone() override;
  void subscribe() override;

  bool commit() override;

protected:
  grpc::ServerAsyncResponseWriter<mhconfig::proto::RunGCResponse> responder_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  mhconfig::proto::RunGCRequest request_;
  mhconfig::proto::RunGCResponse response_;

  void request() override;
  void finish() override;

  run_gc::Type type();
  uint32_t max_live_in_seconds();
};

} /* request */
} /* api */
} /* mhconfig */

#endif
