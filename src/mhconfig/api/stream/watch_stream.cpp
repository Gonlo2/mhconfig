#include "mhconfig/api/stream/watch_stream.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

WatchGetRequest::WatchGetRequest(
  uint32_t version,
  std::shared_ptr<WatchInputMessage> input_message,
  std::shared_ptr<WatchOutputMessage> output_message
) : version_(version),
  input_message_(input_message),
  output_message_(output_message)
{
}

WatchGetRequest::~WatchGetRequest() {
}

const std::string& WatchGetRequest::root_path() const {
  return input_message_->root_path();
}

uint32_t WatchGetRequest::version() const {
  return version_;
}

const Labels& WatchGetRequest::labels() const {
  return input_message_->labels();
}

const std::string& WatchGetRequest::document() const {
  return input_message_->document();
}

void WatchGetRequest::set_status(::mhconfig::api::request::GetRequest::Status status) {
  switch (status) {
    case ::mhconfig::api::request::GetRequest::Status::OK:
      output_message_->set_status(WatchStatus::OK);
      break;
    case ::mhconfig::api::request::GetRequest::Status::ERROR:
      output_message_->set_status(WatchStatus::ERROR);
      break;
    case ::mhconfig::api::request::GetRequest::Status::INVALID_VERSION:
      output_message_->set_status(WatchStatus::INVALID_VERSION);
      break;
    case ::mhconfig::api::request::GetRequest::Status::REF_GRAPH_IS_NOT_DAG:
      output_message_->set_status(WatchStatus::REF_GRAPH_IS_NOT_DAG);
      break;
  }
}

void WatchGetRequest::set_namespace_id(uint64_t namespace_id) {
  output_message_->set_namespace_id(namespace_id);
}

void WatchGetRequest::set_version(uint32_t version) {
  output_message_->set_version(version);
}

void WatchGetRequest::set_element(const mhconfig::Element& element) {
  output_message_->set_element(element);
}

void WatchGetRequest::set_checksum(const uint8_t* data, size_t len) {
  output_message_->set_checksum(data, len);
}

void WatchGetRequest::set_preprocessed_payload(const char* data, size_t len) {
  output_message_->set_preprocessed_payload(data, len);
}

bool WatchGetRequest::commit() {
  return output_message_->commit();
}

} /* stream */
} /* api */
} /* mhconfig */
