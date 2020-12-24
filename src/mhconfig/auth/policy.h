#ifndef MHCONFIG__AUTH__POLICY_H
#define MHCONFIG__AUTH__POLICY_H

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <bits/exception.h>
#include <bits/stdint-uintn.h>
#include <spdlog/fmt/fmt.h>
#include <sys/types.h>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "jmutils/container/label_set.h"
#include "jmutils/time.h"
#include "mhconfig/auth/common.h"
#include "mhconfig/auth/labels_acl.h"
#include "mhconfig/auth/path_acl.h"
#include "mhconfig/element.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

namespace mhconfig
{
namespace auth
{

class Policy final
{
public:
  Policy() {
  }

  ~Policy() {
  }

  bool init(const Element& element) {
    spdlog::trace("Parsing global capabilities");
    auto capabilities = element.get("capabilities");
    if (capabilities.is_undefined()) {
      spdlog::error("A policy must have a capabilities sequence");
      return false;
    }
    if (auto r = parse_capabilities(capabilities); !r) {
      return false;
    } else {
      capabilities_ = *r;
    }

    spdlog::trace("Parsing root_paths capabilities");
    auto root_paths_seq = element.get("root_paths").as_sequence();
    if (root_paths_seq == nullptr) {
      spdlog::error("A policy must have a root_paths sequence");
      return false;
    }
    std::vector<std::pair<std::string, uint8_t>> root_paths;
    for (ssize_t i = root_paths_seq->size()-1; i >= 0; --i) {
      auto path_res = (*root_paths_seq)[i].get("path").template try_as<std::string>();
      if (!path_res) {
        spdlog::error("A root_path policy must have a path scalar");
        return false;
      }

      auto capabilities_res = parse_capabilities(
        (*root_paths_seq)[i].get("capabilities")
      );
      if (!capabilities_res) {
        return false;
      }

      root_paths.emplace_back(*path_res, *capabilities_res);
    }
    if (!path_acl_.init(std::move(root_paths))) {
      spdlog::error("Can't initialize the root paths");
      return false;
    }

    spdlog::trace("Parsing labels capabilities");
    auto labels_seq = element.get("labels").as_sequence();
    if (labels_seq == nullptr) {
      spdlog::error("A policy must have a labels sequence");
      return false;
    }
    for (ssize_t i = labels_seq->size()-1; i >= 0; --i) {
      std::optional<std::string> key;
      auto key_elem = (*labels_seq)[i].get("key");
      if (!key_elem.is_undefined()) {
        key = key_elem.template try_as<std::string>();
        if (!key) {
          spdlog::error("A labels key must be a scalar");
          return false;
        }
      }

      std::optional<std::string> value;
      auto value_elem = (*labels_seq)[i].get("value");
      if (!value_elem.is_undefined()) {
        value = value_elem.template try_as<std::string>();
        if (!value) {
          spdlog::error("A labels value must be a scalar");
          return false;
        }
      }

      auto capabilities_res = parse_capabilities(
        (*labels_seq)[i].get("capabilities")
      );
      if (!capabilities_res) {
        return false;
      }

      labels_acl_.add(std::move(key), std::move(value), *capabilities_res);
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

  AuthResult root_path_auth(
    Capability capability,
    const std::string& root_path
  ) {
    if (!has_capability(capability, capabilities_)) {
      spdlog::trace("Without base capability");
      return AuthResult::PERMISSION_DENIED;
    }

    if (!has_path_capability(capability, root_path)) {
      spdlog::trace("Without root path capability for '{}'", root_path);
      return AuthResult::PERMISSION_DENIED;
    }

    return AuthResult::AUTHENTICATED;
  }

  AuthResult document_auth(
    Capability capability,
    const std::string& root_path,
    const Labels& labels
  ) {
    if (!has_capability(capability, capabilities_)) {
      spdlog::trace("Without base capability");
      return AuthResult::PERMISSION_DENIED;
    }

    if (!has_path_capability(capability, root_path)) {
      spdlog::trace("Without root path capability for '{}'", root_path);
      return AuthResult::PERMISSION_DENIED;
    }

    if (!has_labels_capability(capability, labels)) {
      spdlog::trace("Without labels capability for {}", labels);
      return AuthResult::PERMISSION_DENIED;
    }

    return AuthResult::AUTHENTICATED;
  }

private:
  uint8_t capabilities_;
  PathAcl<uint8_t> path_acl_;
  LabelsAcl labels_acl_;

  inline bool has_labels_capability(
    Capability capability,
    const Labels& labels
  ) {
    auto labels_capabilities = labels_acl_.find(labels);
    return has_capability(capability, labels_capabilities);
  }

  inline bool has_path_capability(
    Capability capability,
    const std::string& path
  ) {
    uint8_t path_capabilities;
    if (!path_acl_.find(path, path_capabilities)) {
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

  std::optional<uint8_t> parse_capabilities(
    const Element& element
  ) {
    auto seq = element.as_sequence();
    if (seq == nullptr) {
      spdlog::error("The capabilities must be a sequence");
      return std::optional<uint8_t>();
    }

    uint8_t capabilities = 0;
    for (const auto& x: *seq) {
      auto res = x.template try_as<std::string>();
      if (!res) {
        spdlog::error("All the capabilities must be literals");
        return std::optional<uint8_t>();
      }

      if (*res == "GET") {
        capabilities |= static_cast<uint8_t>(Capability::GET);
      } else if (*res == "WATCH") {
        capabilities |= static_cast<uint8_t>(Capability::WATCH);
      } else if (*res == "TRACE") {
        capabilities |= static_cast<uint8_t>(Capability::TRACE);
      } else if (*res == "UPDATE") {
        capabilities |= static_cast<uint8_t>(Capability::UPDATE);
      } else if (*res == "RUN_GC") {
        capabilities |= static_cast<uint8_t>(Capability::RUN_GC);
      } else {
        spdlog::error("Unknown entry capability '{}'", *res);
        return std::optional<uint8_t>();
      }
    }

    return std::optional<uint8_t>(capabilities);
  }

};

} /* auth */
} /* mhconfig */

#endif
