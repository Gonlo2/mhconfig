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
    metrics::MetricsService& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : Request(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

RunGCRequestImpl::~RunGCRequestImpl() {
}

const std::string RunGCRequestImpl::name() const {
  return "RUN_GC";
}

std::shared_ptr<Session> RunGCRequestImpl::clone() {
  return make_session<RunGCRequestImpl>(service_, cq_, metrics_, scheduler_queue_);
}

void RunGCRequestImpl::subscribe() {
  service_->RequestRunGC(&ctx_, &request_, &responder_, cq_, cq_, tag());
}

bool RunGCRequestImpl::commit() {
  return reply();
}

void RunGCRequestImpl::request() {
  auto api_run_gc_command = std::make_shared<scheduler::command::RunGcCommand>(
    type(),
    max_live_in_seconds()
  );
  scheduler_queue_.push(api_run_gc_command);

  reply();
}

run_gc::Type RunGCRequestImpl::type() {
  switch (request_.type()) {
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_0:
      return scheduler::command::run_gc::Type::CACHE_GENERATION_0;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_1:
      return scheduler::command::run_gc::Type::CACHE_GENERATION_1;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_2:
      return scheduler::command::run_gc::Type::CACHE_GENERATION_2;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_DEAD_POINTERS:
      return scheduler::command::run_gc::Type::DEAD_POINTERS;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_NAMESPACES:
      return scheduler::command::run_gc::Type::NAMESPACES;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_VERSIONS:
      return scheduler::command::run_gc::Type::VERSIONS;
  }
}

uint32_t RunGCRequestImpl::max_live_in_seconds() {
  return request_.max_live_in_seconds();
}

void RunGCRequestImpl::finish() {
  responder_.Finish(response_, grpc::Status::OK, tag());
}


} /* request */
} /* api */
} /* mhconfig */
