#ifndef MHCONFIG__AUTH__TOKEN_H
#define MHCONFIG__AUTH__TOKEN_H

#include <string>
#include <memory>

#include "jmutils/time.h"

#include "mhconfig/auth/common.h"
#include "mhconfig/auth/entity.h"

namespace mhconfig
{
namespace auth
{

class Token final
{
public:
  Token(
    uint64_t expires_on,
    const std::shared_ptr<Entity>& entity
  ) : expires_on_(expires_on),
    entity_(entity)
  {
  }

  AuthResult basic_auth(
    Capability capability
  ) {
    if (is_expired()) {
      return AuthResult::EXPIRED_TOKEN;
    }

    return entity_->basic_auth(capability);
  }

  template <typename T>
  AuthResult root_path_auth(
    Capability capability,
    const T& request
  ) {
    if (is_expired()) {
      return AuthResult::EXPIRED_TOKEN;
    }

    return entity_->root_path_auth(capability, request);
  }

  template <typename T>
  AuthResult document_auth(
    Capability capability,
    const T& request
  ) {
    if (is_expired()) {
      return AuthResult::EXPIRED_TOKEN;
    }

    return entity_->document_auth(capability, request);
  }

private:
  uint64_t expires_on_;
  std::shared_ptr<Entity> entity_;

  inline bool is_expired() {
    return (expires_on_ != 0) && (jmutils::now_sec() > expires_on_);
  }

};

} /* auth */
} /* mhconfig */

#endif
