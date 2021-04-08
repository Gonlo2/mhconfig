#ifndef MHCONFIG__API__STREAM__WATCH_STREAM_H
#define MHCONFIG__API__STREAM__WATCH_STREAM_H

#include <bits/stdint-uintn.h>
#include <stddef.h>
#include <memory>
#include <optional>
#include <string>

#include "jmutils/container/label_set.h"
#include "mhconfig/api/stream/output_message.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/element.h"

namespace mhconfig
{

class Element;
struct config_namespace_t;

namespace api
{
namespace stream
{

using jmutils::container::Labels;

enum class WatchStatus {
  OK,
  ERROR,
  UID_IN_USE,
  UNKNOWN_UID,
  REMOVED,
  PERMISSION_DENIED,
  INVALID_ARGUMENT
};

class WatchOutputMessage : public OutputMessage
{
public:
  WatchOutputMessage() : OutputMessage() {
  }
  virtual ~WatchOutputMessage() {
  }

  virtual void set_status(WatchStatus status) = 0;
  virtual void set_uid(uint32_t uid) = 0;
  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;

  virtual void set_element(const mhconfig::Element& element) = 0;
  virtual SourceIds set_element_with_position(
    const mhconfig::Element& element
  ) = 0;

  virtual void add_log(
    LogLevel level,
    const std::string_view& message
  ) = 0;
  virtual void add_log(
    LogLevel level,
    const std::string_view& message,
    const position_t& position
  ) = 0;
  virtual void add_log(
    LogLevel level,
    const std::string_view& message,
    const position_t& position,
    const position_t& source
  ) = 0;

  virtual void set_sources(const std::vector<source_t>& sources) = 0;

  virtual void set_checksum(const uint8_t* data, size_t len) = 0;
};

class WatchInputMessage
{
public:
  WatchInputMessage() {
  }
  virtual ~WatchInputMessage() {
  }

  virtual uint32_t uid() const = 0;
  virtual bool remove() const = 0;
  virtual const std::string& root_path() const = 0;
  virtual const Labels& labels() const = 0;
  virtual const std::string& document() const = 0;
  virtual LogLevel log_level() const = 0;
  virtual bool with_position() const = 0;

  virtual std::optional<std::optional<uint64_t>> unregister() = 0;

  virtual std::shared_ptr<WatchOutputMessage> make_output_message() = 0;
};

class WatchGetRequest final
  : public request::GetRequest
{
public:
  WatchGetRequest(
    uint32_t version,
    std::shared_ptr<WatchInputMessage> input_message,
    std::shared_ptr<WatchOutputMessage> output_message
  );
  ~WatchGetRequest();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const Labels& labels() const override;
  const std::string& document() const override;
  LogLevel log_level() const override;
  bool with_position() const override;

  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;

  void set_element(const mhconfig::Element& element) override;
  SourceIds set_element_with_position(
    const mhconfig::Element& element
  ) override;

  void add_log(
    LogLevel level,
    const std::string_view& message
  ) override;
  void add_log(
    LogLevel level,
    const std::string_view& message,
    const position_t& position
  ) override;
  void add_log(
    LogLevel level,
    const std::string_view& message,
    const position_t& position,
    const position_t& source
  ) override;

  void set_sources(const std::vector<source_t>& sources) override;
  void set_checksum(const uint8_t* data, size_t len) override;

  bool commit() override;

private:
  uint32_t version_;
  std::shared_ptr<WatchInputMessage> input_message_;
  std::shared_ptr<WatchOutputMessage> output_message_;
};

} /* stream */
} /* api */
} /* mhconfig */

#endif
