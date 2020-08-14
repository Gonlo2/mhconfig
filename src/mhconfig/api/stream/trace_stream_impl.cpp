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
  response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::TraceResponse>(&arena_);
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

void TraceOutputMessageImpl::set_overrides(const std::vector<std::string>& overrides) {
  response_->clear_overrides();
  for (const auto& s : overrides) {
    response_->add_overrides(s);
  }
}

void TraceOutputMessageImpl::set_flavors(const std::vector<std::string>& flavors) {
  response_->clear_flavors();
  for (const auto& s : flavors) {
    response_->add_flavors(s);
  }
}

void TraceOutputMessageImpl::set_document(const std::string& document) {
  response_->set_document(document);
}

void TraceOutputMessageImpl::set_peer(const std::string& peer) {
  response_->set_peer(peer);
}

bool TraceOutputMessageImpl::send(bool finish) {
  if (auto stream = stream_.lock()) {
    return stream->send(shared_from_this(), finish);
  }
  return false;
}


TraceStreamImpl::TraceStreamImpl()
  : Stream()
{
  request_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::TraceRequest>(&arena_);
}

const std::string& TraceStreamImpl::root_path() const {
  return request_->root_path();
}

const std::vector<std::string>& TraceStreamImpl::overrides() const {
  return overrides_;
}

const std::vector<std::string>& TraceStreamImpl::flavors() const {
  return flavors_;
}

const std::string& TraceStreamImpl::document() const {
  return request_->document();
}

const std::string TraceStreamImpl::name() const {
  return "TRACE";
}

void TraceStreamImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<TraceStreamImpl>()->subscribe(service, cq);
}

void TraceStreamImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = tag(Status::CREATE)) {
    service->RequestTrace(&ctx_, request_, &stream_, cq, cq, t);
  }
}

void TraceStreamImpl::on_create(
  context_t* ctx
) {
  overrides_ = to_vector(request_->overrides());
  flavors_ = to_vector(request_->flavors());

  auto token = get_auth_token();
  auto auth_result = token
    ? ctx->acl.document_auth(*token, auth::Capability::TRACE, *this)
    : auth::AuthResult::UNAUTHENTICATED;

  if (check_auth(auth_result)) {
    bool ok = validator::are_valid_arguments(
      root_path(),
      overrides(),
      flavors(),
      document()
    );

    if (ok) {
      auto cn = get_or_build_cn(ctx, root_path());
      cn->last_access_timestamp = jmutils::monotonic_now_sec();
      process_trace_request<worker::SetupCommand>(
        std::move(cn),
        shared_from_this(),
        ctx
      );
    } else {
      auto output_message = make_output_message();
      output_message->set_status(TraceOutputMessage::Status::ERROR);
      output_message->send(true);
    }
  }
}

void TraceStreamImpl::on_read(
  context_t* ctx
) {
}

std::shared_ptr<TraceOutputMessage> TraceStreamImpl::make_output_message() {
  return std::make_shared<TraceOutputMessageImpl>(shared_from_this());
}

} /* stream */
} /* api */
} /* mhconfig */
