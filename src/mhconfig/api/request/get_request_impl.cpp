#include "mhconfig/api/request/get_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


GetRequestImpl::GetRequestImpl()
  : responder_(&ctx_)
{
  request_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::GetRequest>(&arena_);
  response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::GetResponse>(&arena_);
}

GetRequestImpl::~GetRequestImpl() {
}

const std::string GetRequestImpl::name() const {
  return "GET";
}

const std::string& GetRequestImpl::root_path() const {
  return request_->root_path();
}

uint32_t GetRequestImpl::version() const {
  return request_->version();
}

const std::vector<std::string>& GetRequestImpl::overrides() const {
  return overrides_;
}

const std::vector<std::string>& GetRequestImpl::flavors() const {
  return flavors_;
}

const std::string& GetRequestImpl::document() const {
  return request_->document();
}

std::string GetRequestImpl::peer() const {
  return session_peer();
}

void GetRequestImpl::set_status(Status status) {
  switch (status) {
    case Status::OK:
      response_->set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_OK);
      break;
    case Status::ERROR:
      response_->set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_ERROR);
      break;
    case Status::INVALID_VERSION:
      response_->set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_INVALID_VERSION);
      break;
    case Status::REF_GRAPH_IS_NOT_DAG:
      response_->set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_REF_GRAPH_IS_NOT_DAG);
      break;
  }
}

void GetRequestImpl::set_namespace_id(uint64_t namespace_id) {
  response_->set_namespace_id(namespace_id);
}

void GetRequestImpl::set_version(uint32_t version) {
  response_->set_version(version);
}

void GetRequestImpl::set_element(const mhconfig::Element& element) {
  config::fill_elements(element, response_, response_->add_elements());
}

void GetRequestImpl::set_checksum(const uint8_t* data, size_t len) {
  response_->set_checksum(data, len);
}

void GetRequestImpl::set_preprocessed_payload(const char* data, size_t len) {
  preprocessed_payload_.write(data, len);
}


bool GetRequestImpl::commit() {
  return finish();
}

void GetRequestImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<GetRequestImpl>()->subscribe(service, cq);
}

void GetRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = tag(RequestStatus::CREATE)) {
    service->RequestGet(&ctx_, &raw_request_, &responder_, cq, cq, t);
  }
}

void GetRequestImpl::request(
  context_t* ctx
) {
  auto status = grpc::SerializationTraits<mhconfig::proto::GetRequest>::Deserialize(
    &raw_request_,
    request_
  );
  if (status.ok()) {
    overrides_ = to_vector(request_->overrides());
    flavors_ = to_vector(request_->flavors());

    auto token = get_auth_token();
    auto auth_result = token
      ? ctx->acl.document_auth(*token, auth::Capability::GET, *this)
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
        process_get_request<worker::SetupCommand>(
          std::move(cn),
          shared_from_this(),
          ctx
        );
      } else {
        set_status(Status::ERROR);
        finish();
      }
    }
  } else {
    set_status(Status::ERROR);
    finish();
  }
}

bool GetRequestImpl::finish(const grpc::Status& status) {
  if (auto t = tag(RequestStatus::PROCESS)) {
    if (status.ok()) {
      bool ok = response_->SerializeToOstream(&preprocessed_payload_);
      if (ok) {
        grpc::Slice slice(preprocessed_payload_.str());
        grpc::ByteBuffer raw_response(&slice, 1);

        responder_.Finish(raw_response, status, t);
      } else {
        grpc::Status serialization_error_status(
          grpc::StatusCode::CANCELLED,
          "Some problem takes place serializing the message"
        );
        responder_.FinishWithError(serialization_error_status, t);
      }
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
