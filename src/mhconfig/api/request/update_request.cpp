#include "mhconfig/api/request/update_request.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace update_request
{


UpdateRequest::UpdateRequest(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : Request(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

UpdateRequest::~UpdateRequest() {
}

const std::string UpdateRequest::name() const {
  return "UPDATE";
}

const uint32_t UpdateRequest::id() const {
  return 1;
}

const std::string& UpdateRequest::root_path() const {
  return request_.root_path();
}

const std::vector<std::string>& UpdateRequest::relative_paths() const {
  return relative_paths_;
}


void UpdateRequest::set_namespace_id(uint64_t namespace_id) {
  response_.set_namespace_id(namespace_id);
}

void UpdateRequest::set_status(update_request::Status status) {
  // TODO
  //switch (status) {
    //case Status::OK:
  //}
  //response_.set_status(version);
}

void UpdateRequest::set_version(uint32_t version) {
  response_.set_version(version);
}

Request* UpdateRequest::clone() {
  return new UpdateRequest(service_, cq_, metrics_, scheduler_queue_);
}

void UpdateRequest::subscribe() {
  service_->RequestUpdate(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void UpdateRequest::request() {
  relative_paths_ = to_vector(request_.relative_paths());

  //command::command_t command;
  //command.type = command::CommandType::API;
  //command.api_request = this;

  //scheduler_queue_.push(command);
}

void UpdateRequest::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* update_request */
} /* request */
} /* api */
} /* mhconfig */
