#include "mhconfig/api/request/update_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


UpdateRequestImpl::UpdateRequestImpl()
  : Request(),
  responder_(&ctx_)
{
}

UpdateRequestImpl::~UpdateRequestImpl() {
}

const std::string UpdateRequestImpl::name() const {
  return "UPDATE";
}

const std::string& UpdateRequestImpl::root_path() const {
  return request_.root_path();
}

const std::vector<std::string>& UpdateRequestImpl::relative_paths() const {
  return relative_paths_;
}


void UpdateRequestImpl::set_namespace_id(uint64_t namespace_id) {
  response_.set_namespace_id(namespace_id);
}

void UpdateRequestImpl::set_status(update_request::Status status) {
  switch (status) {
    case update_request::Status::OK:
      response_.set_status(::mhconfig::proto::UpdateResponse_Status::UpdateResponse_Status_OK);
      break;
    case update_request::Status::ERROR:
      response_.set_status(::mhconfig::proto::UpdateResponse_Status::UpdateResponse_Status_ERROR);
      break;
  }
}

void UpdateRequestImpl::set_version(uint32_t version) {
  response_.set_version(version);
}

bool UpdateRequestImpl::commit() {
  return reply();
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
  service->RequestUpdate(&ctx_, &request_, &responder_, cq, cq, tag());
}

void UpdateRequestImpl::request(
  SchedulerQueue::Sender* scheduler_sender
) {
  relative_paths_ = to_vector(request_.relative_paths());

  scheduler_sender->push(
    std::make_unique<scheduler::command::ApiUpdateCommand>(
      shared_from_this()
    )
  );
}

void UpdateRequestImpl::finish() {
  responder_.Finish(response_, grpc::Status::OK, tag());
}


} /* request */
} /* api */
} /* mhconfig */
