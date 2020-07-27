#include "mhconfig/api/request/update_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


UpdateRequestImpl::UpdateRequestImpl()
  : responder_(&ctx_)
{
  request_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::UpdateRequest>(&arena_);
  response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::UpdateResponse>(&arena_);
}

UpdateRequestImpl::~UpdateRequestImpl() {
}

const std::string UpdateRequestImpl::name() const {
  return "UPDATE";
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

void UpdateRequestImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  return make_session<UpdateRequestImpl>()->subscribe(service, cq);
}

void UpdateRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = tag(RequestStatus::CREATE)) {
    service->RequestUpdate(&ctx_, request_, &responder_, cq, cq, t);
  }
}

void UpdateRequestImpl::request(
  auth::Acl* acl,
  SchedulerQueue::Sender* scheduler_sender
) {
  relative_paths_ = to_vector(request_->relative_paths());

  auto token = get_auth_token();
  auto auth_result = token
    ? acl->root_path_auth(*token, auth::Capability::UPDATE, *this)
    : auth::AuthResult::UNAUTHENTICATED;

  if (check_auth(auth_result)) {
    if (validate_request()) {
      scheduler_sender->push(
        std::make_unique<scheduler::ApiUpdateCommand>(
          shared_from_this()
        )
      );
    } else {
      set_status(Status::ERROR);
      finish();
    }
  }
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
