#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_H

#include <string>
#include <vector>

#include "mhconfig/api/request/request.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class GetRequest : public Request
{
public:
  GetRequest(
    CustomService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics
  )
    : Request(service, cq_, metrics)
  {
  };

  virtual ~GetRequest() {};

  virtual const std::string& root_path() const = 0;
  virtual const uint32_t version() const = 0;
  virtual const std::vector<std::string>& overrides() const = 0;
  virtual const std::vector<std::string>& key() const = 0;

  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_version(uint32_t version) = 0;
  virtual void set_element(mhconfig::ElementRef element) = 0;
  virtual void set_element_bytes(const char* data, size_t len) = 0;

};

} /* request */
} /* api */
} /* mhconfig */

#endif
