#include "mhconfig/auth/acl.h"

namespace mhconfig
{
namespace auth
{

std::optional<std::vector<std::string>> to_string_vector(const YAML::Node& node) {
  if (!node || !node.IsSequence()) return std::optional<std::vector<std::string>>();
  std::vector<std::string> result;
  result.reserve(node.size());
  for (const auto& it : node) {
    if (!it.IsScalar()) return std::optional<std::vector<std::string>>();
    result.push_back(it.as<std::string>());
  }
  return std::optional<std::vector<std::string>>(result);
}

} /* auth */
} /* mhconfig */
