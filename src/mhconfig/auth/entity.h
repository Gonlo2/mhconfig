#ifndef MHCONFIG__AUTH__ENTITY_H
#define MHCONFIG__AUTH__ENTITY_H

#include "mhconfig/auth/common.h"
#include "mhconfig/auth/path_container.h"

namespace mhconfig
{
namespace auth
{

class Entity
{
public:
  Entity() {
  }

  virtual ~Entity() {
  }

  bool init(
    uint8_t capabilities,
    std::vector<std::pair<std::string, uint8_t>>&& root_path,
    std::vector<std::pair<std::string, uint8_t>>&& overrides
  ) {
    capabilities_ = capabilities;

    if (!capabilities_by_root_path_.init(root_path)) {
      return false;
    }

    if (!capabilities_by_override_.init(overrides)) {
      return false;
    }

    return true;
  }

  AuthResult basic_auth(
    Capability capability
  ) {
    if (!has_capability(capability, capabilities_)) {
      spdlog::trace("Without base capability");
      return AuthResult::PERMISSION_DENIED;
    }

    return AuthResult::AUTHENTICATED;
  }

  template <typename T>
  AuthResult root_path_auth(
    Capability capability,
    const T& request
  ) {
    if (!has_capability(capability, capabilities_)) {
      spdlog::trace("Without base capability");
      return AuthResult::PERMISSION_DENIED;
    }

    if (!has_capability(capability, request.root_path(), capabilities_by_root_path_)) {
      spdlog::trace("Without root path capability for '{}'", request.root_path());
      return AuthResult::PERMISSION_DENIED;
    }

    return AuthResult::AUTHENTICATED;
  }

  template <typename T>
  AuthResult document_auth(
    Capability capability,
    const T& request
  ) {
    if (!has_capability(capability, capabilities_)) {
      spdlog::trace("Without base capability");
      return AuthResult::PERMISSION_DENIED;
    }

    if (!has_capability(capability, request.root_path(), capabilities_by_root_path_)) {
      spdlog::trace("Without root path capability for '{}'", request.root_path());
      return AuthResult::PERMISSION_DENIED;
    }

    for (const auto& override_ : request.overrides()) {
      if (!has_capability(capability, override_, capabilities_by_override_)) {
        spdlog::trace("Without override capability for '{}'", override_);
        return AuthResult::PERMISSION_DENIED;
      }
    }

    return AuthResult::AUTHENTICATED;
  }

private:
  uint8_t capabilities_;
  PathContainer<uint8_t> capabilities_by_root_path_;
  PathContainer<uint8_t> capabilities_by_override_;

  inline bool has_capability(
    Capability capability,
    const std::string& path,
    const PathContainer<uint8_t>& path_container
  ) {
    uint8_t path_capabilities;
    if (!path_container.find(path, path_capabilities)) {
      return false;
    }
    return has_capability(capability, path_capabilities);
  }

  inline bool has_capability(
    Capability capability,
    uint8_t capabilities
  ) {
    return (capabilities & static_cast<uint8_t>(capability)) == static_cast<uint8_t>(capability);
  }

};

} /* auth */
} /* mhconfig */

#endif
