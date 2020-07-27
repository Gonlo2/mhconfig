#ifndef MHCONFIG__AUTH__ACL_H
#define MHCONFIG__AUTH__ACL_H

#include <filesystem>
#include <string>

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "jmutils/time.h"

#include "mhconfig/auth/common.h"
#include "mhconfig/auth/entity.h"
#include "mhconfig/auth/token.h"

namespace mhconfig
{
namespace auth
{

std::optional<std::vector<std::string>> to_string_vector(const YAML::Node& node);

class Acl
{
public:
  Acl() {
  }

  virtual ~Acl() {
  }

  bool load(const std::string& path) {
    absl::MutexLock lock(&load_mutex_);

    try {
      auto raw_policies_opt = load_raw_policies(path + "/policy");
      if (!raw_policies_opt) return false;

      auto raw_entities_opt = load_raw_entities(path + "/entity");
      if (!raw_entities_opt) return false;

      auto raw_tokens_opt = load_raw_tokens(path + "/token");
      if (!raw_tokens_opt) return false;

      absl::flat_hash_map<std::string, std::shared_ptr<Entity>> entity_by_id;
      entity_by_id.reserve(raw_entities_opt->size());
      for (const auto& raw_entity : *raw_entities_opt) {
        raw_policy_t merged_raw_policy;
        for (const auto& policy_id : raw_entity.policies) {
          auto search = raw_policies_opt->find(policy_id);
          if (search == raw_policies_opt->end()) {
            spdlog::error(
              "The entity '{}' try to use the non-existent policy '{}'",
              raw_entity.entity,
              policy_id
            );
            return false;
          }

          merged_raw_policy.root_path.insert(
            merged_raw_policy.root_path.end(),
            search->second.root_path.cbegin(),
            search->second.root_path.cend()
          );

          merged_raw_policy.overrides.insert(
            merged_raw_policy.overrides.end(),
            search->second.overrides.cbegin(),
            search->second.overrides.cend()
          );
        }

        auto entity = std::make_shared<Entity>();
        bool ok = entity->init(
          raw_entity.capabilities,
          to_vector(merged_raw_policy.root_path),
          to_vector(merged_raw_policy.overrides)
        );
        if (!ok) return false;

        entity_by_id.emplace(raw_entity.entity, std::move(entity));
      }

      absl::flat_hash_map<std::string, std::unique_ptr<Token>> token_by_id;
      token_by_id.reserve(raw_tokens_opt->size());
      for (const auto& raw_token : *raw_tokens_opt) {
        auto search = entity_by_id.find(raw_token.entity);
        if (search == entity_by_id.end()) {
          spdlog::error(
            "The token '{}' try to use the non-existent entity '{}'",
            raw_token.token,
            raw_token.entity
          );
          return false;
        }

        token_by_id.emplace(
          raw_token.token,
          std::make_unique<Token>(
            raw_token.expires_on,
            search->second
          )
        );
      }

      mutex_.Lock();
      token_by_id_ = std::move(token_by_id);
      mutex_.Unlock();

      return true;
    } catch (const std::exception &e) {
      spdlog::error("Some error take place loading the acl config: {}", e.what());
    } catch (...) {
      spdlog::error("Some unknown error take place loading the acl config");
    }

    return false;
  }

  AuthResult basic_auth(
    const std::string& token,
    Capability capability
  ) {
    absl::ReaderMutexLock lock(&mutex_);
    auto search = token_by_id_.find(token);
    if (search != token_by_id_.end()) {
      return search->second->basic_auth(capability);
    }
    return AuthResult::UNAUTHENTICATED;
  }

  template <typename T>
  AuthResult root_path_auth(
    const std::string& token,
    Capability capability,
    const T& request
  ) {
    absl::ReaderMutexLock lock(&mutex_);
    auto search = token_by_id_.find(token);
    if (search != token_by_id_.end()) {
      return search->second->root_path_auth(capability, request);
    }
    return AuthResult::UNAUTHENTICATED;
  }

  template <typename T>
  AuthResult document_auth(
    const std::string& token,
    Capability capability,
    const T& request
  ) {
    absl::ReaderMutexLock lock(&mutex_);
    auto search = token_by_id_.find(token);
    if (search != token_by_id_.end()) {
      return search->second->document_auth(capability, request);
    }
    return AuthResult::UNAUTHENTICATED;
  }

private:
  struct raw_path_t {
    std::string path;
    uint8_t capabilities;
  };

  struct raw_policy_t {
    std::vector<raw_path_t> root_path;
    std::vector<raw_path_t> overrides;
  };

  struct raw_entity_t {
    std::string entity;
    uint8_t capabilities;
    std::vector<std::string> policies;
  };

  struct raw_token_t {
    std::string token;
    uint64_t expires_on;
    std::string entity;
  };

  absl::Mutex mutex_;
  absl::Mutex load_mutex_;
  absl::flat_hash_map<std::string, std::unique_ptr<Token>> token_by_id_;

  std::vector<std::pair<std::string, uint8_t>> to_vector(
    const std::vector<raw_path_t>& raw_paths
  ) {
    std::vector<std::pair<std::string, uint8_t>> result;
    result.reserve(raw_paths.size());
    for (const auto& raw_path : raw_paths) {
      result.emplace_back(raw_path.path, raw_path.capabilities);
    }
    return result;
  }

  std::optional<absl::flat_hash_map<std::string, raw_policy_t>> load_raw_policies(
    const std::string& path
  ) {
    absl::flat_hash_map<std::string, raw_policy_t> raw_policies;
    bool ok = config_files_iterator(
      path,
      [&raw_policies, this](const auto& path, auto&& config) {
        auto& raw_policy = raw_policies[path.stem().string()];

        if (!add_paths("root_path", path.string(), config["root_path"], raw_policy.root_path)) {
          return false;
        }

        if (!add_paths("overrides", path.string(), config["overrides"], raw_policy.overrides)) {
          return false;
        }

        return true;
      }
    );
    if (!ok) return std::optional<absl::flat_hash_map<std::string, raw_policy_t>>();
    return std::optional<absl::flat_hash_map<std::string, raw_policy_t>>(raw_policies);
  }

  std::optional<std::vector<raw_entity_t>> load_raw_entities(
    const std::string& path
  ) {
    std::vector<raw_entity_t> raw_entities;
    bool ok = config_files_iterator(
      path,
      [&raw_entities, this](const auto& path, auto&& config) {
        raw_entity_t raw_entity;

        raw_entity.entity = path.stem().string();

        auto capabilities_opt = parse_capabilities(
          "entity",
          path.string(),
          config["capabilities"]
        );
        if (!capabilities_opt) return false;
        raw_entity.capabilities = *capabilities_opt;

        auto policies_node = config["policies"];
        if (policies_node) {
          auto policies_opt = to_string_vector(policies_node);
          if (!policies_opt) {
            spdlog::error("The policies must be a literal sequence (file: '{}')", path.string());
            return false;
          }
          raw_entity.policies = *policies_opt;
        }

        raw_entities.push_back(std::move(raw_entity));
        return true;
      }
    );
    if (!ok) return std::optional<std::vector<raw_entity_t>>();
    return std::optional<std::vector<raw_entity_t>>(raw_entities);
  }

  std::optional<std::vector<raw_token_t>> load_raw_tokens(
    const std::string& path
  ) {
    std::vector<raw_token_t> raw_tokens;
    bool ok = config_files_iterator(
      path,
      [&raw_tokens, this](const auto& path, auto&& config) {
        raw_token_t raw_token;

        raw_token.token = path.stem().string();

        auto expires_on_node = config["expires_on"];
        if (expires_on_node) {
          if (!expires_on_node.IsScalar()) {
            spdlog::error(
              "The expires_on must be a unix timestamp (file: '{}')",
              path.string()
            );
            return false;
          }
          raw_token.expires_on = expires_on_node.template as<uint64_t>();
        } else {
          raw_token.expires_on = 0;
        }

        auto entity_node = config["entity"];
        if (!entity_node || !entity_node.IsScalar()) {
          spdlog::error(
            "The entity must be a literal (file: '{}')",
            path.string()
          );
          return false;
        }
        raw_token.entity = entity_node.template as<std::string>();

        raw_tokens.push_back(raw_token);
        return true;
      }
    );
    if (!ok) return std::optional<std::vector<raw_token_t>>();
    return std::optional<std::vector<raw_token_t>>(raw_tokens);
  }

  template <typename F>
  bool config_files_iterator(
    const std::string& path,
    F lambda
  ) {
    for ( std::filesystem::directory_iterator it(path), end; it != end; ++it) {
      if (
          (it->path().filename().native()[0] != '.')
          && it->is_regular_file()
          && (it->path().extension() == ".yaml")
      ) {
        if (!lambda(it->path(), YAML::LoadFile(it->path()))) {
          return false;
        }
      }
    }

    return true;
  }

  bool add_paths(
    const std::string& name,
    const std::string& node_path,
    const YAML::Node& node,
    std::vector<raw_path_t>& paths
  ) {
    if (!node) return true;
    if (!node.IsSequence()) {
      spdlog::error("The {} must be a sequence (file: '{}')", name, node_path);
      return false;
    }
    for (const auto& it: node) {
      raw_path_t raw_path;

      auto& path_node = it["path"];
      if (!path_node || !path_node.IsScalar()) {
        spdlog::error(
          "All the {} entries must have a 'path' literal (file: '{}')",
          name,
          node_path
        );
        return false;
      }
      raw_path.path = path_node.template as<std::string>();

      auto& capabilities_node = it["capabilities"];
      if (!capabilities_node || !capabilities_node.IsSequence()) {
        spdlog::error(
          "All the {} entries must have a 'capabilities' sequence (file: '{}')",
          name,
          node_path
        );
        return false;
      }

      auto capabilities_opt = parse_capabilities(name, node_path, capabilities_node);
      if (!capabilities_opt) return false;
      raw_path.capabilities = *capabilities_opt;

      paths.push_back(raw_path);
    }

    return true;
  }

  std::optional<uint8_t> parse_capabilities(
    const std::string& name,
    const std::string& node_path,
    const YAML::Node& node
  ) {
    uint8_t capabilities = 0;
    for (const auto& capability_node : node) {
      if (!capability_node.IsScalar()) {
        spdlog::error(
          "All the {} entries 'capabilities' must be literals (file: '{}')",
          name,
          node_path
        );
        return std::optional<uint8_t>();
      }

      auto capability = capability_node.template as<std::string>();
      if (capability == "GET") {
        capabilities |= static_cast<uint8_t>(Capability::GET);
      } else if (capability == "WATCH") {
        capabilities |= static_cast<uint8_t>(Capability::WATCH);
      } else if (capability == "TRACE") {
        capabilities |= static_cast<uint8_t>(Capability::TRACE);
      } else if (capability == "UPDATE") {
        capabilities |= static_cast<uint8_t>(Capability::UPDATE);
      } else if (capability == "RUN_GC") {
        capabilities |= static_cast<uint8_t>(Capability::RUN_GC);
      } else {
        spdlog::error(
          "Unknown {} entry capability '{}' (file: '{}')",
          name,
          capability,
          node_path
        );
        return std::optional<uint8_t>();
      }
    }

    return std::optional<uint8_t>(capabilities);
  }

};

} /* auth */
} /* mhconfig */

#endif
