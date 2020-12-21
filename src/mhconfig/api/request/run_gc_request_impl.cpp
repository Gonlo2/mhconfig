#include "mhconfig/api/request/run_gc_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{

RunGCRequestImpl::~RunGCRequestImpl() {
}

bool RunGCRequestImpl::commit() {
  return finish();
}

void RunGCRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestRunGC(&server_ctx_, request_, &responder_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> RunGCRequestImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<RunGCRequestImpl>(ctx_)->subscribe(service, cq);
  return nullptr;
}

std::shared_ptr<PolicyCheck> RunGCRequestImpl::parse_message() {
  return shared_from_this();
}

void RunGCRequestImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    auth_result = policy->basic_auth(
      auth::Capability::RUN_GC
    );
    if (check_auth(auth_result)) {
      if (auto command = make_gc_command()) {
        ctx_->worker_queue.push(std::move(command));
      }

      finish();
    }
  }
}

void RunGCRequestImpl::on_check_policy_error() {
  finish_with_unknown();
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
  if (auto t = make_tag(GrpcStatus::WRITE)) {
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
