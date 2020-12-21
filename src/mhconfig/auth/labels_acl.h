#ifndef MHCONFIG__AUTH__LABEL_ACL_H
#define MHCONFIG__AUTH__LABEL_ACL_H

#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include "spdlog/spdlog.h"
#include "mhconfig/auth/common.h"
#include "jmutils/common.h"
#include "jmutils/container/label_set.h"

namespace mhconfig
{
namespace auth
{

using jmutils::container::Labels;

class LabelsAcl final
{
public:
  LabelsAcl() {
  }

  ~LabelsAcl() {
  }

  void add(
    std::optional<std::string>&& key,
    std::optional<std::string>&& value,
    uint8_t capabilities
  ) {
    if (key) {
      auto& node = by_key_[*key];
      if (value) {
        spdlog::trace("Adding capabilities {} for {}/{}", capabilities, *key, *value);
        node.by_value[*value] = capabilities;
      } else {
        spdlog::trace("Adding capabilities {} for {}", capabilities, *key);
        node.fallback = capabilities;
        node.by_value.clear();
      }
    } else {
      spdlog::trace("Adding capabilities {} for all the labels", capabilities);
      fallback_ = capabilities;
      by_key_.clear();
    }
  }

  uint8_t find(const Labels& labels) const {
    if (labels.empty()) return fallback_;

    uint8_t capabilities = 0xff;
    for (const auto& label : labels) {
      auto ksearch = by_key_.find(label.first);
      if (ksearch == by_key_.end()) {
        spdlog::trace(
          "The label '{}' don't have specific capabilities, using default {}",
          label.first,
          fallback_
        );
        capabilities &= fallback_;
      } else {
        auto vsearch = ksearch->second.by_value .find(label.second);
        if (vsearch == ksearch->second.by_value.end()) {
          spdlog::trace(
            "The label '{}/{}' don't have specific capabilities, using default {}",
            label.first,
            label.second,
            ksearch->second.fallback
          );
          capabilities &= ksearch->second.fallback;
        } else{
          spdlog::trace(
            "The label '{}/{}' have specific capabilities {}",
            label.first,
            label.second,
            vsearch->second
          );
          capabilities &= vsearch->second;
        }
      }
    }
    return capabilities;
  }

private:
  struct value_capabilities_t {
    uint8_t fallback{0};
    absl::flat_hash_map<std::string, uint8_t> by_value;
  };

  uint8_t fallback_{0};
  absl::flat_hash_map<std::string, value_capabilities_t> by_key_;
};

} /* auth */
} /* mhconfig */

#endif
