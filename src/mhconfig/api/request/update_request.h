#ifndef MHCONFIG__API__REQUEST__UPDATE_REQUEST_H
#define MHCONFIG__API__REQUEST__UPDATE_REQUEST_H

#include <string>
#include <vector>

#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace update_request
{

enum Status {
  OK,
  ERROR
};

} /* update_request */


class UpdateRequest : public Request
{
public:
  UpdateRequest(
    mhconfig::proto::MHConfig::AsyncService* service,
    grpc::ServerCompletionQueue* cq_,
    Metrics& metrics
  )
    : Request(service, cq_, metrics)
  {
  };

  virtual ~UpdateRequest() {
  };

  virtual const std::string& root_path() const = 0;
  virtual const std::vector<std::string>& relative_paths() const = 0;

  virtual void set_namespace_id(uint64_t namespace_id) = 0;
  virtual void set_status(update_request::Status status) = 0;
  virtual void set_version(uint32_t version) = 0;
};

} /* request */
} /* api */
} /* mhconfig */

#endif
