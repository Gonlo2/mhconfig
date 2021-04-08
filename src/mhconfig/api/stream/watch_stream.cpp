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

LogLevel WatchGetRequest::log_level() const {
  return input_message_->log_level();
}

bool WatchGetRequest::with_position() const {
    return input_message_->with_position();
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

SourceIds WatchGetRequest::set_element_with_position(
  const mhconfig::Element& element
) {
  return output_message_->set_element_with_position(element);
}

void WatchGetRequest::add_log(
  LogLevel level,
  const std::string_view& message
) {
  output_message_->add_log(level, message);
}

void WatchGetRequest::add_log(
  LogLevel level,
  const std::string_view& message,
  const position_t& position
) {
  output_message_->add_log(level, message, position);
}

void WatchGetRequest::add_log(
  LogLevel level,
  const std::string_view& message,
  const position_t& position,
  const position_t& source
) {
  output_message_->add_log(level, message, position, source);
}

void WatchGetRequest::set_sources(
  const std::vector<source_t>& sources
) {
  output_message_->set_sources(sources);
}

void WatchGetRequest::set_checksum(const uint8_t* data, size_t len) {
  output_message_->set_checksum(data, len);
}

bool WatchGetRequest::commit() {
  return output_message_->commit();
}

} /* stream */
} /* api */
} /* mhconfig */
