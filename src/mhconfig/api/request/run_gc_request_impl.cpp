#include "mhconfig/api/request/run_gc_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{

RunGCRequestImpl::RunGCRequestImpl(
    CustomService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : RunGCRequest(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

RunGCRequestImpl::~RunGCRequestImpl() {
}

const std::string RunGCRequestImpl::name() const {
  return "RUN_GC";
}

Request* RunGCRequestImpl::clone() {
  return new RunGCRequestImpl(service_, cq_, metrics_, scheduler_queue_);
}

void RunGCRequestImpl::subscribe() {
  service_->RequestRunGC(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void RunGCRequestImpl::request() {
  //TODO
}

void RunGCRequestImpl::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* request */
} /* api */
} /* mhconfig */
