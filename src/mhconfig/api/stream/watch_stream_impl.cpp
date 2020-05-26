#include "mhconfig/api/stream/watch_stream_impl.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

WatchOutputMessageImpl::WatchOutputMessageImpl(
  std::weak_ptr<WatchStreamImpl>& stream
)
  : stream_(stream)
{
  proto_response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::WatchResponse>(&arena_);
}

WatchOutputMessageImpl::~WatchOutputMessageImpl() {
}

void WatchOutputMessageImpl::set_uid(uint32_t uid) {
  proto_response_->set_uid(uid);
}

void WatchOutputMessageImpl::set_status(watch::Status status) {
  switch (status) {
    case watch::Status::OK:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_OK);
      break;
    case watch::Status::ERROR:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_ERROR);
      break;
    case watch::Status::INVALID_VERSION:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_INVALID_VERSION);
      break;
    case watch::Status::REF_GRAPH_IS_NOT_DAG:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_REF_GRAPH_IS_NOT_DAG);
      break;
    case watch::Status::UID_IN_USE:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_UID_IN_USE);
      break;
    case watch::Status::UNKNOWN_UID:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_UNKNOWN_UID);
      break;
    case watch::Status::REMOVED:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_REMOVED);
      break;
  }
}

void WatchOutputMessageImpl::set_namespace_id(uint64_t namespace_id) {
  proto_response_->set_namespace_id(namespace_id);
}

void WatchOutputMessageImpl::set_version(uint32_t version) {
  proto_response_->set_version(version);
}

void WatchOutputMessageImpl::set_element(mhconfig::Element* element) {
  elements_data_.clear();
  proto_response_->clear_elements();
  mhconfig::api::config::fill_elements(
    element,
    proto_response_,
    proto_response_->add_elements()
  );
}

void WatchOutputMessageImpl::set_element_bytes(const char* data, size_t len) {
  elements_data_.clear();
  elements_data_.write(data, len);
  proto_response_->clear_elements();
}

bool WatchOutputMessageImpl::send(bool finish) {
  if (auto stream = stream_.lock()) {
    if (proto_response_->SerializeToOstream(&elements_data_)) {
      slice_ = grpc::Slice(elements_data_.str());
      response_ = grpc::ByteBuffer(&slice_, 1);
      return stream->send(shared_from_this(), finish);
    }
  }
  return false;
}


WatchInputMessageImpl::WatchInputMessageImpl(
  std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
  std::weak_ptr<WatchStreamImpl>&& stream
)
  : request_(std::move(request)),
    stream_(std::move(stream))
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

bool WatchInputMessageImpl::unregister() {
  if (auto stream = stream_.lock()) {
    return stream->unregister(uid());
  }
  return false;
}

std::shared_ptr<WatchOutputMessage> WatchInputMessageImpl::make_output_message() {
  auto msg = std::make_shared<WatchOutputMessageImpl>(stream_);
  msg->set_uid(request_->uid());
  return msg;
}


WatchGetRequest::WatchGetRequest(
  std::shared_ptr<WatchInputMessage> input_message,
  std::shared_ptr<WatchOutputMessage> output_message
)
  : input_message_(input_message),
  output_message_(output_message)
{
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

const std::string& WatchGetRequest::document() const {
  return input_message_->document();
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

void WatchGetRequest::set_element(mhconfig::Element* element) {
  output_message_->set_element(element);
}

void WatchGetRequest::set_element_bytes(const char* data, size_t len) {
  output_message_->set_element_bytes(data, len);
}

bool WatchGetRequest::commit() {
  return output_message_->commit();
}


WatchStreamImpl::WatchStreamImpl()
  : Stream()
{
}

WatchStreamImpl::~WatchStreamImpl() {
}

const std::string WatchStreamImpl::name() const {
  return "WATCH";
}

void WatchStreamImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  return make_session<WatchStreamImpl>()->subscribe(service, cq);
}

void WatchStreamImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  service->RequestWatch(&ctx_, &stream_, cq, cq, tag());
}

bool WatchStreamImpl::unregister(uint32_t uid) {
  std::lock_guard<std::recursive_mutex> mlock(mutex_);
  return watcher_by_id_.erase(uid);
}

void WatchStreamImpl::prepare_next_request() {
  next_req_.Clear();
  stream_.Read(&next_req_, tag());
}

void WatchStreamImpl::request(
  SchedulerQueue::Sender* scheduler_sender
) {
  auto req = std::make_unique<mhconfig::proto::WatchRequest>();
  auto status = grpc::SerializationTraits<mhconfig::proto::WatchRequest>::Deserialize(
    &next_req_,
    req.get()
  );
  auto msg = std::make_shared<WatchInputMessageImpl>(std::move(req), shared_from_this());
  if (status.ok()) {
    if (msg->remove()) {
      spdlog::debug("Removing watcher with uid {} of the stream {}", msg->uid(), tag());
      size_t removed_elements = watcher_by_id_.erase(msg->uid());
      auto out_msg = msg->make_output_message();
      out_msg->set_status(
        (removed_elements == 0) ? watch::Status::UNKNOWN_UID : watch::Status::REMOVED
      );
      out_msg->send();
    } else {
      spdlog::debug("Adding watcher with uid {} to the stream {}", msg->uid(), tag());
      auto inserted = watcher_by_id_.emplace(msg->uid(), msg);
      if (inserted.second) {
        scheduler_sender->push(
          std::make_unique<scheduler::command::ApiWatchCommand>(
            msg
          )
        );
      } else {
        auto out_msg = msg->make_output_message();
        out_msg->set_status(watch::Status::UID_IN_USE);
        out_msg->send();
      }
    }
  } else {
    auto out_msg = msg->make_output_message();
    out_msg->set_status(watch::Status::ERROR);
    out_msg->send(true); // We probably don't know the uid so we need to finish the stream u.u
  }
}

} /* stream */
} /* api */
} /* mhconfig */
