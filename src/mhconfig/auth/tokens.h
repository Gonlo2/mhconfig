#ifndef MHCONFIG__AUTH__TOKENS_H
#define MHCONFIG__AUTH__TOKENS_H

#include <filesystem>
#include <string>

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"

#include "jmutils/time.h"

#include "mhconfig/auth/common.h"
#include "mhconfig/auth/path_acl.h"
#include "mhconfig/auth/labels_acl.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace auth
{

using jmutils::container::label_t;

class Tokens final
{
public:
  Tokens() {
  }

  ~Tokens() {
  }

  bool init(
    const Element& element
  ) {
    auto tokens_seq = element.get("tokens").as_sequence();
    if (tokens_seq == nullptr) {
      spdlog::error("The tokens must be a sequence");
      return false;
    }

    for (auto& t: *tokens_seq) {
      auto id_res = t.get("value").try_as<std::string>();
      if (!id_res) {
        spdlog::error("Some token value isn't a scalar");
        return false;
      }

      auto& token = tokens_[*id_res];

      if (auto res = t.get("expire_at").try_as<int64_t>(); !res) {
        spdlog::error(
          "The expire_at of the token '{}' isn't a valid integer",
          *id_res
        );
        return false;
      } else {
        token.expire_at = *res;
      }

      auto labels_map = t.get("labels").as_map();
      if (labels_map == nullptr) {
        spdlog::error(
          "The labels of the token '{}' isn't a valid map",
          *id_res
        );
        return false;
      } else {
        std::vector<label_t> labels_tmp;
        for (auto& x : *labels_map) {
          if (auto res = x.second.try_as<std::string>(); !res) {
            spdlog::error(
              "The label '{}' value of the token '{}' isn't a valid scalar",
              x.first.str(),
              *id_res
            );
            return false;
          } else {
            labels_tmp.emplace_back(x.first.str(), *res);
          }
        }
        token.labels = jmutils::container::make_labels(std::move(labels_tmp));
      }
    }

    return true;
  }

  AuthResult find(
    const std::string& value,
    Labels& labels
  ) {
    auto search = tokens_.find(value);
    if (search == tokens_.end()) {
      spdlog::debug("Without the token '{}'", value);
      return AuthResult::UNAUTHENTICATED;
    }

    if (search->second.expire_at) {
      if (jmutils::now_sec() > search->second.expire_at) {
        spdlog::debug("The token '{}' has expired", value);
        return AuthResult::EXPIRED_TOKEN;
      }
    }

    labels = search->second.labels;

    return AuthResult::AUTHENTICATED;
  }

private:
  struct token_t {
    uint64_t expire_at;
    Labels labels;
  };

  absl::flat_hash_map<std::string, token_t> tokens_;

};

} /* auth */
} /* mhconfig */

#endif
