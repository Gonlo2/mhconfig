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
    Queue<command::command_t>& scheduler_queue
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

const uint32_t RunGCRequest::id() const {
  return 2;
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
  worker::command::command_t command;
  command.type = worker::command::CommandType::RUN_GC_REQUEST;
  command.run_gc_request = std::make_shared<worker::command::run_gc::request_t>();
  command.run_gc_request->max_live_in_seconds = request_.max_live_in_seconds();

  switch (request_.type()) {
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_0:
      command.run_gc_request->type = worker::command::run_gc::Type::CACHE_GENERATION_0;
      break;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_1:
      command.run_gc_request->type = worker::command::run_gc::Type::CACHE_GENERATION_1;
      break;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_2:
      command.run_gc_request->type = worker::command::run_gc::Type::CACHE_GENERATION_2;
      break;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_DEAD_POINTERS:
      command.run_gc_request->type = worker::command::run_gc::Type::DEAD_POINTERS;
      break;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_NAMESPACES:
      command.run_gc_request->type = worker::command::run_gc::Type::NAMESPACES;
      break;
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_VERSIONS:
      command.run_gc_request->type = worker::command::run_gc::Type::VERSIONS;
      break;
    default:
      logger_->error("Unknown RunGC type {}", request_.type());
      return;
  }

  scheduler_queue_.push(command);
}

void RunGCRequest::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* run_gc_request */
} /* request */
} /* api */
} /* mhconfig */
