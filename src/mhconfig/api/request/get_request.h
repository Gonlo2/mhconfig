#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_H

#include <string>
#include <vector>

#include "jmutils/container/label_set.h"
#include "mhconfig/api/commitable.h"
#include "mhconfig/api/common.h"

namespace mhconfig
{
namespace api
{
namespace request
{

using jmutils::container::Labels;

class GetRequest : public Commitable
{
public:
  GetRequest() {};
  virtual ~GetRequest() {};

  virtual const std::string& root_path() const = 0;
  virtual uint32_t version() const = 0;
  virtual const Labels& labels() const = 0;
  virtual const std::string& document() const = 0;
  virtual LogLevel log_level() const = 0;
  virtual bool with_position() const = 0;

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

} /* request */
} /* api */
} /* mhconfig */

#endif
