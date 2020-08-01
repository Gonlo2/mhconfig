#include "mhconfig/scheduler/common.h"

namespace mhconfig
{
namespace scheduler
{

bool are_valid_arguments(
  const std::vector<std::string>& overrides,
  const std::vector<std::string>& flavors,
  const std::string& document
) {
  if (!builder::is_a_valid_document_name(document)) {
    spdlog::error("The document '{}' don't have a valid name", document);
    return false;
  }

  absl::flat_hash_set<std::string> repeated_elements;

  for (size_t i = 0, l = overrides.size(); i < l; ++i) {
    if (!repeated_elements.insert(overrides[i]).second) {
      spdlog::error("The override '{}' is repeated", overrides[i]);
      return false;
    }
  }

  repeated_elements.clear();

  for (size_t i = 0, l = flavors.size(); i < l; ++i) {
    if (!repeated_elements.insert(flavors[i]).second) {
      spdlog::error("The flavor '{}' is repeated", flavors[i]);
      return false;
    }
  }

  return true;
}

} /* scheduler */
} /* mhconfig */
