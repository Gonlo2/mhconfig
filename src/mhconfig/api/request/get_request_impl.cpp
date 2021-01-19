#include "mhconfig/api/request/get_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


GetRequestImpl::~GetRequestImpl() {
}

const std::string& GetRequestImpl::root_path() const {
  return request_->root_path();
}

uint32_t GetRequestImpl::version() const {
  return request_->version();
}

const Labels& GetRequestImpl::labels() const {
  return labels_;
}

const std::string& GetRequestImpl::document() const {
  return request_->document();
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
  response_->clear_elements();
  config::fill_elements(element, response_, response_->add_elements());
}

void GetRequestImpl::set_checksum(const uint8_t* data, size_t len) {
  response_->set_checksum(data, len);
}


bool GetRequestImpl::commit() {
  return finish();
}

void GetRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestGet(&server_ctx_, request_, &responder_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> GetRequestImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<GetRequestImpl>(ctx_)->subscribe(service, cq);
  return nullptr;
}

std::shared_ptr<PolicyCheck> GetRequestImpl::parse_message() {
  labels_ = to_labels(request_->labels());
  return shared_from_this();
}

void GetRequestImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    auth_result = policy->document_auth(
      auth::Capability::GET,
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
        process_get_config_task(
          std::move(cn),
          std::make_shared<ApiGetConfigTask>(shared_from_this()),
          ctx_.get()
        );
      } else {
        finish_with_invalid_argument();
      }
    }
  }
}

void GetRequestImpl::on_check_policy_error() {
  finish_with_unknown();
}

bool GetRequestImpl::finish(const grpc::Status& status) {
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
