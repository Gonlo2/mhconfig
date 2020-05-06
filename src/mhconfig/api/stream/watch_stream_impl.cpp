#include "mhconfig/api/stream/watch_stream_impl.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

WatchOutputMessageImpl::WatchOutputMessageImpl(
  std::shared_ptr<WatchStreamImpl> stream
)
  : stream_(stream)
{
}

WatchOutputMessageImpl::~WatchOutputMessageImpl() {
}

void WatchOutputMessageImpl::set_uid(uint32_t uid) {
  proto_response_.set_uid(uid);
}

void WatchOutputMessageImpl::set_status(watch::Status status) {
  switch (status) {
    case watch::Status::OK:
      proto_response_.set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_OK);
      break;
    case watch::Status::ERROR:
      proto_response_.set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_ERROR);
      break;
    case watch::Status::INVALID_VERSION:
      proto_response_.set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_INVALID_VERSION);
      break;
    case watch::Status::REF_GRAPH_IS_NOT_DAG:
      proto_response_.set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_REF_GRAPH_IS_NOT_DAG);
      break;
    case watch::Status::UID_IN_USE:
      proto_response_.set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_UID_IN_USE);
      break;
  }
}

void WatchOutputMessageImpl::set_namespace_id(uint64_t namespace_id) {
  proto_response_.set_namespace_id(namespace_id);
}

void WatchOutputMessageImpl::set_version(uint32_t version) {
  proto_response_.set_version(version);
}

void WatchOutputMessageImpl::set_element(mhconfig::ElementRef element) {
  elements_data_.clear();
  proto_response_.clear_elements();
  mhconfig::api::config::fill_elements(element, &proto_response_, proto_response_.add_elements());
}

void WatchOutputMessageImpl::set_element_bytes(const char* data, size_t len) {
  elements_data_.clear();
  elements_data_.write(data, len);
  proto_response_.clear_elements();
}

bool WatchOutputMessageImpl::send(bool finish) {
  bool ok = proto_response_.SerializeToOstream(&elements_data_);
  if (ok) {
    slice_ = grpc::Slice(elements_data_.str());
    response_ = grpc::ByteBuffer(&slice_, 1);

    return stream_->send(shared_from_this(), finish);
  }

  //TODO add support to close the stream without a message
  return stream_->send(shared_from_this(), true);
  return false;
}


WatchInputMessageImpl::WatchInputMessageImpl(
  std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
  std::shared_ptr<WatchStreamImpl> stream
)
  : request_(std::move(request)),
    stream_(stream)
{
  overrides_ = to_vector(request_->overrides());
}


WatchInputMessageImpl::~WatchInputMessageImpl() {
}

const uint32_t WatchInputMessageImpl::uid() const {
  return request_->uid();
}

const bool WatchInputMessageImpl::remove() const {
  return request_->remove();
}

const std::string& WatchInputMessageImpl::root_path() const {
  return request_->root_path();
}

const std::vector<std::string>& WatchInputMessageImpl::overrides() const {
  return overrides_;
}

const uint32_t WatchInputMessageImpl::version() const {
  return request_->version();
}

const std::string& WatchInputMessageImpl::document() const {
  return request_->document();
}

std::shared_ptr<WatchOutputMessage> WatchInputMessageImpl::make_output_message() {
  return std::make_shared<WatchOutputMessageImpl>(stream_);
}


WatchGetRequest::WatchGetRequest(
  std::shared_ptr<WatchInputMessage> input_message,
  std::shared_ptr<WatchOutputMessage> output_message
)
  : input_message_(input_message),
  output_message_(output_message)
{
  key_.push_back(input_message_->document());
}

WatchGetRequest::~WatchGetRequest() {
}

const std::string& WatchGetRequest::root_path() const {
  return input_message_->root_path();
}

const uint32_t WatchGetRequest::version() const {
  return input_message_->version();
}

const std::vector<std::string>& WatchGetRequest::overrides() const {
  return input_message_->overrides();
}

const std::vector<std::string>& WatchGetRequest::key() const {
  return key_;
}

void WatchGetRequest::set_status(::mhconfig::api::request::get_request::Status status) {
  switch (status) {
    case ::mhconfig::api::request::get_request::Status::OK:
      output_message_->set_status(watch::Status::OK);
      break;
    case ::mhconfig::api::request::get_request::Status::ERROR:
      output_message_->set_status(watch::Status::ERROR);
      break;
    case ::mhconfig::api::request::get_request::Status::INVALID_VERSION:
      output_message_->set_status(watch::Status::INVALID_VERSION);
      break;
    case ::mhconfig::api::request::get_request::Status::REF_GRAPH_IS_NOT_DAG:
      output_message_->set_status(watch::Status::REF_GRAPH_IS_NOT_DAG);
      break;
  }
}

void WatchGetRequest::set_namespace_id(uint64_t namespace_id) {
  output_message_->set_namespace_id(namespace_id);
}

void WatchGetRequest::set_version(uint32_t version) {
  output_message_->set_version(version);
}

void WatchGetRequest::set_element(mhconfig::ElementRef element) {
  output_message_->set_element(element);
}

void WatchGetRequest::set_element_bytes(const char* data, size_t len) {
  output_message_->set_element_bytes(data, len);
}

bool WatchGetRequest::commit() {
  return output_message_->commit();
}


WatchStreamImpl::WatchStreamImpl(
  CustomService* service,
  grpc::ServerCompletionQueue* cq,
  Metrics& metrics,
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
)
  : Stream(service, cq, metrics),
  scheduler_queue_(scheduler_queue)
{
}

WatchStreamImpl::~WatchStreamImpl() {
}

const std::string WatchStreamImpl::name() const {
  return "WATCH";
}

std::shared_ptr<Session> WatchStreamImpl::clone() {
  return make_session<WatchStreamImpl>(service_, cq_, metrics_, scheduler_queue_);
}

void WatchStreamImpl::subscribe() {
  service_->RequestWatch(&ctx_, &stream_, cq_, cq_, tag());
}

void WatchStreamImpl::request(std::unique_ptr<grpc::ByteBuffer>&& raw_req) {
  auto req = std::make_unique<mhconfig::proto::WatchRequest>();
  bool ok = parse_from_byte_buffer(*raw_req, *req);
  auto msg = std::make_shared<WatchInputMessageImpl>(std::move(req), shared_from_this());
  if (ok) {
    auto inserted = watcher_by_id_.emplace(msg->uid(), msg);
    if (inserted.second) {
      auto api_watch_command = std::make_shared<scheduler::command::ApiWatchCommand>(
        msg
      );
      scheduler_queue_.push(api_watch_command);
    } else {
      auto out_msg = msg->make_output_message();
      out_msg->set_status(watch::Status::UID_IN_USE);
      out_msg->send();
    }
  } else {
    auto out_msg = msg->make_output_message();
    out_msg->set_uid(msg->uid());
    out_msg->set_status(watch::Status::ERROR);
    out_msg->send(true); // We probably don't know the uid so we need to finish the stream u.u
  }
}

} /* stream */
} /* api */
} /* mhconfig */
