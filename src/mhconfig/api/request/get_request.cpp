#include "mhconfig/api/request/get_request.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace get_request
{


GetRequest::GetRequest(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<command::command_t>& scheduler_queue
) : Request(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

GetRequest::~GetRequest() {
}

const std::string GetRequest::name() const {
  return "GET";
}

const uint32_t GetRequest::id() const {
  return 0;
}

const std::string& GetRequest::root_path() const {
  return request_.root_path();
}

const uint32_t GetRequest::version() const {
  return request_.version();
}

const std::vector<std::string>& GetRequest::overrides() const {
  return overrides_;
}

const std::vector<std::string>& GetRequest::key() const {
  return key_;
}


void GetRequest::set_namespace_id(uint64_t namespace_id) {
  response_.set_namespace_id(namespace_id);
}

void GetRequest::set_version(uint32_t version) {
  response_.set_version(version);
}

void GetRequest::set_element(mhconfig::ElementRef element) {
  response_.clear_elements();
  mhconfig::api::config::fill_elements(element, &response_, response_.add_elements());
}

mhconfig::proto::GetResponse& GetRequest::response() {
  return response_;
}

Request* GetRequest::clone() {
  return new GetRequest(service_, cq_, metrics_, scheduler_queue_);
}

void GetRequest::subscribe() {
  service_->RequestGet(&ctx_, &request_, &responder_, cq_, cq_, this);
}

void GetRequest::request() {
  overrides_ = to_vector(request_.overrides());
  key_ = to_vector(request_.key());

  command::command_t command;
  command.type = command::CommandType::API;
  command.api_request = this;

  scheduler_queue_.push(command);
}

void GetRequest::finish() {
  responder_.Finish(response_, grpc::Status::OK, this);
}


} /* get_request */
} /* request */
} /* api */
} /* mhconfig */
