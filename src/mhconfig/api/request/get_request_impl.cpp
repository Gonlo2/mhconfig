#include "mhconfig/api/request/get_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


GetRequestImpl::GetRequestImpl(
    CustomService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : GetRequest(service, cq_, metrics),
    responder_(&ctx_),
    scheduler_queue_(scheduler_queue)
{
}

GetRequestImpl::~GetRequestImpl() {
}

const std::string GetRequestImpl::name() const {
  return "GET";
}

const std::string& GetRequestImpl::root_path() const {
  return request_.root_path();
}

const uint32_t GetRequestImpl::version() const {
  return request_.version();
}

const std::vector<std::string>& GetRequestImpl::overrides() const {
  return overrides_;
}

const std::vector<std::string>& GetRequestImpl::key() const {
  return key_;
}


void GetRequestImpl::set_namespace_id(uint64_t namespace_id) {
  response_.set_namespace_id(namespace_id);
}

void GetRequestImpl::set_version(uint32_t version) {
  response_.set_version(version);
}

void GetRequestImpl::set_element(mhconfig::ElementRef element) {
  elements_data_.clear();
  response_.clear_elements();
  mhconfig::api::config::fill_elements(element, &response_, response_.add_elements());
}

void GetRequestImpl::set_element_bytes(const std::string& data) {
  elements_data_.clear();
  elements_data_ << data;
  response_.clear_elements();
}

Request* GetRequestImpl::clone() {
  return new GetRequestImpl(service_, cq_, metrics_, scheduler_queue_);
}

void GetRequestImpl::subscribe() {
  service_->RequestGet(&ctx_, &raw_request_, &responder_, cq_, cq_, this);
}

void GetRequestImpl::request() {
  parse_from_byte_buffer(raw_request_, request_); //TODO check error result

  overrides_ = to_vector(request_.overrides());
  key_ = to_vector(request_.key());

  auto api_get_command = std::make_shared<scheduler::command::ApiGetCommand>(
    static_cast<::mhconfig::api::request::GetRequest*>(this)
  );
  scheduler_queue_.push(api_get_command);
}

void GetRequestImpl::finish() {
  response_.SerializeToOstream(&elements_data_);  //TODO check error

  grpc::Slice slice(elements_data_.str());
  grpc::ByteBuffer raw_response(&slice, 1);

  responder_.Finish(raw_response, grpc::Status::OK, this);
}


} /* request */
} /* api */
} /* mhconfig */
