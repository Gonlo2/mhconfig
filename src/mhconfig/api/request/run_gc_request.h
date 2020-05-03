#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_H

#include "mhconfig/api/session.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class RunGCRequest : public Commitable
{
public:
  RunGCRequest() {}
  virtual ~RunGCRequest() {}
};

} /* request */
} /* api */
} /* mhconfig */

#endif
