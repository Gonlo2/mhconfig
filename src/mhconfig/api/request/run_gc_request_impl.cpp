#include "mhconfig/api/request/run_gc_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{

RunGCRequestImpl::RunGCRequestImpl()
  : responder_(&ctx_)
{
  request_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::RunGCRequest>(&arena_);
  response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::RunGCResponse>(&arena_);
}

RunGCRequestImpl::~RunGCRequestImpl() {
}

const std::string RunGCRequestImpl::name() const {
  return "RUN_GC";
}

void RunGCRequestImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  return make_session<RunGCRequestImpl>()->subscribe(service, cq);
}

void RunGCRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  service->RequestRunGC(&ctx_, request_, &responder_, cq, cq, tag());
}

bool RunGCRequestImpl::commit() {
  return reply();
}

void RunGCRequestImpl::request(
  SchedulerQueue::Sender* scheduler_sender
) {
  scheduler_sender->push(
    std::make_unique<scheduler::command::RunGcCommand>(
      type(),
      max_live_in_seconds()
    )
  );

  reply();
}

scheduler::command::RunGcCommand::Type RunGCRequestImpl::type() {
  switch (request_->type()) {
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_0:
      return scheduler::command::RunGcCommand::Type::CACHE_GENERATION_0;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_1:
      return scheduler::command::RunGcCommand::Type::CACHE_GENERATION_1;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_2:
      return scheduler::command::RunGcCommand::Type::CACHE_GENERATION_2;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_DEAD_POINTERS:
      return scheduler::command::RunGcCommand::Type::DEAD_POINTERS;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_NAMESPACES:
      return scheduler::command::RunGcCommand::Type::NAMESPACES;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_VERSIONS:
      return scheduler::command::RunGcCommand::Type::VERSIONS;
  }
}

uint32_t RunGCRequestImpl::max_live_in_seconds() {
  return request_->max_live_in_seconds();
}

void RunGCRequestImpl::finish() {
  responder_.Finish(*response_, grpc::Status::OK, tag());
}


} /* request */
} /* api */
} /* mhconfig */
