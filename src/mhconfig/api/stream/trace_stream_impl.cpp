#include "mhconfig/api/stream/trace_stream_impl.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

TraceOutputMessageImpl::TraceOutputMessageImpl(
  std::weak_ptr<TraceStreamImpl>&& stream
)
  : stream_(std::move(stream))
{
  response_ = Arena::CreateMessage<mhconfig::proto::TraceResponse>(&arena_);
}

void TraceOutputMessageImpl::set_status(Status status) {
  switch (status) {
    case Status::RETURNED_ELEMENTS:
      response_->set_status(::mhconfig::proto::TraceResponse_Status::TraceResponse_Status_RETURNED_ELEMENTS);
      break;
    case Status::ERROR:
      response_->set_status(::mhconfig::proto::TraceResponse_Status::TraceResponse_Status_ERROR);
      break;
    case Status::ADDED_WATCHER:
      response_->set_status(::mhconfig::proto::TraceResponse_Status::TraceResponse_Status_ADDED_WATCHER);
      break;
    case Status::EXISTING_WATCHER:
      response_->set_status(::mhconfig::proto::TraceResponse_Status::TraceResponse_Status_EXISTING_WATCHER);
      break;
    case Status::REMOVED_WATCHER:
      response_->set_status(::mhconfig::proto::TraceResponse_Status::TraceResponse_Status_REMOVED_WATCHER);
      break;
  }
}

void TraceOutputMessageImpl::set_namespace_id(uint64_t namespace_id) {
  response_->set_namespace_id(namespace_id);
}

void TraceOutputMessageImpl::set_version(uint32_t version) {
  response_->set_version(version);
}

void TraceOutputMessageImpl::set_labels(const Labels& labels) {
  response_->clear_labels();
  for (const auto& l : labels) {
    auto ll = response_->add_labels();
    ll->set_key(l.first);
    ll->set_value(l.second);
  }
}

void TraceOutputMessageImpl::set_document(const std::string& document) {
  response_->set_document(document);
}

bool TraceOutputMessageImpl::send(bool finish) {
  if (auto stream = stream_.lock()) {
    return stream->send(shared_from_this(), finish);
  }
  return false;
}

const std::string& TraceStreamImpl::root_path() const {
  return request_->root_path();
}

const Labels& TraceStreamImpl::labels() const {
  return labels_;
}

const std::string& TraceStreamImpl::document() const {
  return request_->document();
}

void TraceStreamImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestTrace(&server_ctx_, request_, &stream_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> TraceStreamImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<TraceStreamImpl>(ctx_)->subscribe(service, cq);
  return nullptr;
}

std::shared_ptr<PolicyCheck> TraceStreamImpl::parse_message() {
  labels_ = to_labels(request_->labels());
  return shared_from_this();
}

void TraceStreamImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    auth_result = policy->document_auth(
      auth::Capability::TRACE,
      root_path(),
      labels()
    );
    if (check_auth(auth_result)) {
      bool ok = validator::are_valid_arguments(
        root_path(),
        labels(),
        document()
      );

      if (ok) {
        auto cn = get_or_build_cn(ctx_.get(), root_path());
        process_trace_request(
          std::move(cn),
          shared_from_this(),
          ctx_.get()
        );
      } else {
        finish_with_invalid_argument();
      }
    }
  }
}

void TraceStreamImpl::on_check_policy_error() {
  finish_with_unknown();
}

std::shared_ptr<TraceOutputMessage> TraceStreamImpl::make_output_message() {
  return std::make_shared<TraceOutputMessageImpl>(shared_from_this());
}

} /* stream */
} /* api */
} /* mhconfig */
