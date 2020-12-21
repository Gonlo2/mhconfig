#ifndef MHCONFIG__API__STREAM__TRACE_STREAM_H
#define MHCONFIG__API__STREAM__TRACE_STREAM_H

#include <bits/stdint-uintn.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "jmutils/container/label_set.h"
#include "mhconfig/api/stream/output_message.h"


namespace mhconfig
{
namespace api
{
namespace stream
{

using jmutils::container::Labels;

class TraceOutputMessage : public OutputMessage
{
public:
  enum class Status {
    RETURNED_ELEMENTS,
    ERROR,
    ADDED_WATCHER,
    EXISTING_WATCHER,
    REMOVED_WATCHER
  };

  TraceOutputMessage() : OutputMessage() {
  }
  virtual ~TraceOutputMessage() {
  }

  virtual void set_status(Status status) = 0;
  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;
  virtual void set_labels(const Labels& labels) = 0;
  virtual void set_document(const std::string& document) = 0;
};

class TraceInputMessage
{
public:
  TraceInputMessage() {
  }
  virtual ~TraceInputMessage() {
  }

  virtual const std::string& root_path() const = 0;
  virtual const Labels& labels() const = 0;
  virtual const std::string& document() const = 0;

  virtual std::shared_ptr<TraceOutputMessage> make_output_message() = 0;
};


} /* stream */
} /* api */
} /* mhconfig */

#endif
