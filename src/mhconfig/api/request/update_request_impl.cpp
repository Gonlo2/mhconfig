#include "mhconfig/api/request/update_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{

UpdateRequestImpl::~UpdateRequestImpl() {
}

const std::string& UpdateRequestImpl::root_path() const {
  return request_->root_path();
}

const std::vector<std::string>& UpdateRequestImpl::relative_paths() const {
  return relative_paths_;
}

bool UpdateRequestImpl::reload() const {
  return request_->reload();
}

void UpdateRequestImpl::set_namespace_id(uint64_t namespace_id) {
  response_->set_namespace_id(namespace_id);
}

void UpdateRequestImpl::set_status(Status status) {
  switch (status) {
    case Status::OK:
      response_->set_status(::mhconfig::proto::UpdateResponse_Status::UpdateResponse_Status_OK);
      break;
    case Status::ERROR:
      response_->set_status(::mhconfig::proto::UpdateResponse_Status::UpdateResponse_Status_ERROR);
      break;
  }
}

void UpdateRequestImpl::set_version(uint32_t version) {
  response_->set_version(version);
}

bool UpdateRequestImpl::commit() {
  return finish();
}

void UpdateRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestUpdate(&server_ctx_, request_, &responder_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> UpdateRequestImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<UpdateRequestImpl>(ctx_)->subscribe(service, cq);
  return nullptr;
}

std::shared_ptr<PolicyCheck> UpdateRequestImpl::parse_message() {
  relative_paths_ = to_vector(request_->relative_paths());
  return shared_from_this();
}

void UpdateRequestImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    auth_result = policy->root_path_auth(
      auth::Capability::UPDATE,
      root_path()
    );
    if (check_auth(auth_result)) {
      if (validate_request()) {
        auto cn = get_or_build_cn(ctx_.get(), root_path());
        process_update_request(
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

void UpdateRequestImpl::on_check_policy_error() {
  finish_with_unknown();
}

bool UpdateRequestImpl::validate_request() {
  if (!validator::is_a_valid_absolute_path(root_path())) {
    spdlog::error("The root path '{}' isn't valid", root_path());
    return false;
  }

  for (size_t i = 0, l = relative_paths_.size(); i < l; ++i) {
    if (!validator::is_a_valid_relative_path(relative_paths_[i])) {
      spdlog::error("The path '{}' isn't a valid relative path", relative_paths_[i]);
      return false;
    }
  }

  return true;
}

bool UpdateRequestImpl::finish(const grpc::Status& status) {
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
