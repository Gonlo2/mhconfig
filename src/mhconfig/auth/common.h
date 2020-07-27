#ifndef MHCONFIG__AUTH__COMMON_H
#define MHCONFIG__AUTH__COMMON_H

#include <string>

namespace mhconfig
{
namespace auth
{

enum class AuthResult {
  AUTHENTICATED,
  UNAUTHENTICATED,
  PERMISSION_DENIED,
  EXPIRED_TOKEN
};

enum class Capability {
  GET = 1<<0,
  WATCH = 1<<1,
  TRACE = 1<<2,
  UPDATE = 1<<3,
  RUN_GC = 1<<4
};

} /* auth */
} /* mhconfig */

#endif
