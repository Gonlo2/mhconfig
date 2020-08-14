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
  if (auto t = tag(RequestStatus::CREATE)) {
    service->RequestRunGC(&ctx_, request_, &responder_, cq, cq, t);
  }
}

bool RunGCRequestImpl::commit() {
  return finish();
}

void RunGCRequestImpl::request(
  context_t* ctx
) {
  auto token = get_auth_token();
  auto auth_result = token
    ? ctx->acl.basic_auth(*token, auth::Capability::RUN_GC)
    : auth::AuthResult::UNAUTHENTICATED;

  if (check_auth(auth_result)) {
    if (auto command = make_gc_command(); command != nullptr) {
      ctx->worker_queue.push(std::move(command));
    }

    finish();
  };
}

std::unique_ptr<WorkerCommand> RunGCRequestImpl::make_gc_command() {
  auto now = jmutils::monotonic_now_sec();
  auto timelimit_s = now < max_live_in_seconds() ? 0 : now - max_live_in_seconds();

  switch (request_->type()) {
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_0:
      return std::make_unique<worker::GCMergedConfigsCommand>(0, timelimit_s);
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_1:
      return std::make_unique<worker::GCMergedConfigsCommand>(1, timelimit_s);
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_CACHE_GENERATION_2:
      return std::make_unique<worker::GCMergedConfigsCommand>(2, timelimit_s);
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_DEAD_POINTERS:
      return std::make_unique<worker::GCDeadPointersCommand>();
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_NAMESPACES:
      return std::make_unique<worker::GCConfigNamespacesCommand>(timelimit_s);
    case mhconfig::proto::RunGCRequest::Type::RunGCRequest_Type_VERSIONS:
      return std::make_unique<worker::GCRawConfigVersionsCommand>(timelimit_s);
  }

  return nullptr;
}

uint32_t RunGCRequestImpl::max_live_in_seconds() {
  return request_->max_live_in_seconds();
}

bool RunGCRequestImpl::finish(const grpc::Status& status) {
  if (auto t = tag(RequestStatus::PROCESS)) {
    if (status.ok()) {
      responder_.Finish(*response_, status, t);
    } else {
      responder_.FinishWithError(status, t);
    }
    return true;
  }

  return false;
}


} /* request */
} /* api */
} /* mhconfig */
