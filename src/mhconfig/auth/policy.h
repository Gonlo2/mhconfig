#ifndef MHCONFIG__AUTH__POLICY_H
#define MHCONFIG__AUTH__POLICY_H

#include <absl/synchronization/mutex.h>

#include "mhconfig/auth/common.h"

namespace mhconfig
{
namespace auth
{

class Policy
{
public:
  Policy() {
  }

  virtual ~Policy() {
  }

  template <typename T>
  AuthResult check_auth(
    Capability capability,
    const T& request
  ) {
    return AuthResult::NOT_AUTHORIZED;
  }

private:
};

} /* auth */
} /* mhconfig */

#endif
