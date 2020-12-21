#ifndef MHCONFIG__VALIDATOR_H
#define MHCONFIG__VALIDATOR_H

#include <stddef.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "jmutils/container/label_set.h"
#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace validator
{

using jmutils::container::Labels;

bool are_valid_arguments(
  const std::string& root_path,
  const Labels& labels,
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
  return true; //TODO
}

} /* validator */
} /* mhconfig */

#endif
