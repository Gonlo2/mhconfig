#ifndef MHCONFIG__API__CONFIG__MERGED_CONFIG_H
#define MHCONFIG__API__CONFIG__MERGED_CONFIG_H

#include "mhconfig/api/request/get_request.h"

namespace mhconfig
{
namespace api
{

namespace config
{

class MergedConfig
{
public:
  MergedConfig() {};
  virtual ~MergedConfig() {};

  virtual void add_elements(
    request::GetRequest* api_request
  ) = 0;
};

} /* config */
} /* api */
} /* mhconfig */

#endif
