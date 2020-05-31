#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_H

#include <string>
#include <vector>

#include "mhconfig/api/commitable.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class GetRequest : public Commitable
{
public:
  enum Status {
    OK,
    ERROR,
    INVALID_VERSION,
    REF_GRAPH_IS_NOT_DAG
  };

  GetRequest() {};
  virtual ~GetRequest() {};

  virtual const std::string& root_path() const = 0;
  virtual uint32_t version() const = 0;
  virtual const std::vector<std::string>& overrides() const = 0;
  virtual const std::string& document() const = 0;
  virtual const std::string& template_() const = 0;

  virtual void set_status(Status status) = 0;
  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;
  virtual void set_element(mhconfig::Element* element) = 0;
  virtual void set_element_bytes(const char* data, size_t len) = 0;
  virtual void set_template_rendered(const std::string& data) = 0;
};

} /* request */
} /* api */
} /* mhconfig */

#endif
