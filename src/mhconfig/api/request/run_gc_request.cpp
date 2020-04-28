#include "mhconfig/api/request/run_gc_request.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace run_gc_request
{

RunGCRequest::RunGCRequest(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : Request(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

RunGCRequest::~RunGCRequest() {
}

const std::string RunGCRequest::name() const {
  return "RUN_GC";
}

Request* RunGCRequest::clone() {
  return new RunGCRequest(service_, cq_, metrics_, scheduler_queue_);
}

void RunGCRequest::subscribe() {
  service_->RequestRunGC(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void RunGCRequest::request() {
  notify_scheduler_if_possible();
  reply();
}

void RunGCRequest::notify_scheduler_if_possible() {
}

void RunGCRequest::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* run_gc_request */
} /* request */
} /* api */
} /* mhconfig */
