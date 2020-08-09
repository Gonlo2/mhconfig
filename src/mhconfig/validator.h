#ifndef MHCONFIG__VALIDATOR_H
#define MHCONFIG__VALIDATOR_H

#include <vector>
#include <string>
#include <memory>

#include <absl/container/flat_hash_set.h>

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace validator
{

bool are_valid_arguments(
  const std::string& root_path,
  const std::vector<std::string>& overrides,
  const std::vector<std::string>& flavors,
  const std::string& document
);

bool is_a_valid_relative_path(
  std::string_view path
);

inline bool is_a_valid_absolute_path(
  std::string_view path
) {
  if (path.empty() || (path.front() != '/')) return false;
  path.remove_prefix(1);
  return is_a_valid_relative_path(path);
}

inline bool is_a_valid_document_name(const std::string& document) {
  for (size_t i = 0, l = document.size(); i < l; ++i) {
    if (document[i] == '/') return false;
  }
  return true;
}

inline bool is_a_valid_path(const std::string& path) {
  return true;
}

inline bool is_a_valid_flavor(const std::string& flavor) {
  for (size_t i = 0, l = flavor.size(); i < l; ++i) {
    if (flavor[i] == '/') return false;
  }
  return true;
}

} /* validator */
} /* mhconfig */

#endif
