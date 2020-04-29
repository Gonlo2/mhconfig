#include "mhconfig/api/request/update_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


UpdateRequestImpl::UpdateRequestImpl(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : UpdateRequest(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
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
  // TODO
  //switch (status) {
    //case Status::OK:
  //}
  //response_.set_status(version);
}

void UpdateRequestImpl::set_version(uint32_t version) {
  response_.set_version(version);
}

Request* UpdateRequestImpl::clone() {
  return new UpdateRequestImpl(service_, cq_, metrics_, scheduler_queue_);
}

void UpdateRequestImpl::subscribe() {
  service_->RequestUpdate(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void UpdateRequestImpl::request() {
  relative_paths_ = to_vector(request_.relative_paths());

  auto api_update_command = std::make_shared<scheduler::command::ApiUpdateCommand>(
    static_cast<::mhconfig::api::request::UpdateRequest*>(this)
  );
  scheduler_queue_.push(api_update_command);
}

void UpdateRequestImpl::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* request */
} /* api */
} /* mhconfig */
