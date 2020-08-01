#ifndef MHCONFIG__VALIDATOR_H
#define MHCONFIG__VALIDATOR_H

#include <vector>
#include <string>
#include <memory>

#include <absl/container/flat_hash_set.h>

#include "mhconfig/builder.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace validator
{

bool is_a_valid_absolute_path(
  std::string_view path
);

bool is_a_valid_relative_path(
  std::string_view path
);

bool are_valid_arguments(
  const std::string& root_path,
  const std::vector<std::string>& overrides,
  const std::vector<std::string>& flavors,
  const std::string& document
);

} /* validator */
} /* mhconfig */

#endif
