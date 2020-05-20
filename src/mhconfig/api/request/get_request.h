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
namespace get_request
{

enum Status {
  OK,
  ERROR,
  INVALID_VERSION,
  REF_GRAPH_IS_NOT_DAG
};

} /* get_request */

class GetRequest : public Commitable
{
public:
  GetRequest() {};
  virtual ~GetRequest() {};

  virtual const std::string& root_path() const = 0;
  virtual const uint32_t version() const = 0;
  virtual const std::vector<std::string>& overrides() const = 0;
  virtual const std::string& document() const = 0;
  virtual const std::vector<std::string>& key() const = 0;

  virtual void set_status(get_request::Status status) = 0;
  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;
  virtual void set_element(mhconfig::ElementRef element) = 0;
  virtual void set_element_bytes(const char* data, size_t len) = 0;
};

} /* request */
} /* api */
} /* mhconfig */

#endif
