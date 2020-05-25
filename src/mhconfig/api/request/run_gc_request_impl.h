#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H

#include "mhconfig/common.h"
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

using namespace mhconfig::scheduler::command;

class RunGCRequestImpl : public Request, public RunGCRequest, public std::enable_shared_from_this<RunGCRequestImpl>
{
public:
  RunGCRequestImpl();
  virtual ~RunGCRequestImpl();

  const std::string name() const override;

  void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

  bool commit() override;

protected:
  google::protobuf::Arena arena_;
  grpc::ServerAsyncResponseWriter<mhconfig::proto::RunGCResponse> responder_;

  mhconfig::proto::RunGCRequest* request_;
  mhconfig::proto::RunGCResponse* response_;

  void request(
    SchedulerQueue::Sender* scheduler_sender
  ) override;
  void finish() override;

  run_gc::Type type();
  uint32_t max_live_in_seconds();
};

} /* request */
} /* api */
} /* mhconfig */

#endif
