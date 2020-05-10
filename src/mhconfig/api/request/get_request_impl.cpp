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
    metrics::MetricsService& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
) : Request(service, cq_, metrics),
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

void GetRequestImpl::set_status(get_request::Status status) {
  switch (status) {
    case get_request::Status::OK:
      response_.set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_OK);
      break;
    case get_request::Status::ERROR:
      response_.set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_ERROR);
      break;
    case get_request::Status::INVALID_VERSION:
      response_.set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_INVALID_VERSION);
      break;
    case get_request::Status::REF_GRAPH_IS_NOT_DAG:
      response_.set_status(::mhconfig::proto::GetResponse_Status::GetResponse_Status_REF_GRAPH_IS_NOT_DAG);
      break;
  }
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

void GetRequestImpl::set_element_bytes(const char* data, size_t len) {
  elements_data_.clear();
  elements_data_.write(data, len);
  response_.clear_elements();
}

bool GetRequestImpl::commit() {
  return reply();
}

std::shared_ptr<Session> GetRequestImpl::clone() {
  return make_session<GetRequestImpl>(service_, cq_, metrics_, scheduler_queue_);
}

void GetRequestImpl::subscribe() {
  service_->RequestGet(&ctx_, &raw_request_, &responder_, cq_, cq_, tag());
}

void GetRequestImpl::request() {
  bool ok = parse_from_byte_buffer(raw_request_, request_);
  if (ok) {
    overrides_ = to_vector(request_.overrides());
    key_ = to_vector(request_.key());

    auto api_get_command = std::make_shared<scheduler::command::ApiGetCommand>(
      shared_from_this()
    );
    scheduler_queue_.push(api_get_command);
  } else {
    set_element(UNDEFINED_ELEMENT);
    reply();
  }
}

void GetRequestImpl::finish() {
  bool ok = response_.SerializeToOstream(&elements_data_);
  if (ok) {
    grpc::Slice slice(elements_data_.str());
    grpc::ByteBuffer raw_response(&slice, 1);

    responder_.Finish(raw_response, grpc::Status::OK, tag());
  } else {
    grpc::Status status(
      grpc::StatusCode::CANCELLED,
      "Some problem takes place serializing the message"
    );
    responder_.FinishWithError(status, tag());
  }
}


} /* request */
} /* api */
} /* mhconfig */
