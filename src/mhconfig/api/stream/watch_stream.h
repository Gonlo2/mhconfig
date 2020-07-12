#ifndef MHCONFIG__API__STREAM__WATCH_STREAM_H
#define MHCONFIG__API__STREAM__WATCH_STREAM_H

#include "mhconfig/api/stream/output_message.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

enum class WatchStatus {
  OK,
  ERROR,
  INVALID_VERSION,
  REF_GRAPH_IS_NOT_DAG,
  UID_IN_USE,
  UNKNOWN_UID,
  REMOVED
};

class WatchOutputMessage : public OutputMessage
{
public:
  WatchOutputMessage() : OutputMessage() {
  }
  virtual ~WatchOutputMessage() {
  }

  virtual void set_uid(uint32_t uid) = 0;
  virtual void set_status(WatchStatus status) = 0;
  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;
  virtual void set_element(const mhconfig::Element& element) = 0;
  virtual void set_element_bytes(const char* data, size_t len) = 0;
  virtual void set_template_rendered(const std::string& data) = 0;
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
  virtual const std::vector<std::string>& overrides() const = 0;
  virtual const std::vector<std::string>& flavors() const = 0;
  virtual uint32_t version() const = 0;
  virtual const std::string& document() const = 0;
  virtual const std::string& template_() const = 0;

  virtual bool unregister() = 0;

  virtual std::shared_ptr<WatchOutputMessage> make_output_message() = 0;
};


} /* stream */
} /* api */
} /* mhconfig */

#endif
