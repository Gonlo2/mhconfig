#include "mhconfig/validator.h"

namespace mhconfig
{
namespace validator
{

bool is_a_valid_absolute_path(
  std::string_view path
) {
  if (path.empty() || (path.front() != '/')) return false;
  path.remove_prefix(1);
  return is_a_valid_relative_path(path);
}

bool is_a_valid_relative_path(
  std::string_view path
) {
  while (!path.empty()) {
    if (path.front() == '/') {
      return false;
    }

    if (path.front() == '.') {
      path.remove_prefix(1);
      if (path.empty() || (path.front() == '/')) {
        return false;
      } else if (path.front() == '.') {
        path.remove_prefix(1);
        if (path.empty() || (path.front() == '/')) {
          return false;
        }
      }
    }

    do {
      path.remove_prefix(1);
    } while (!path.empty() && (path.front() != '/'));

    if (!path.empty()) path.remove_prefix(1);
  }

  return true;
}

bool are_valid_arguments(
  const std::string& root_path,
  const std::vector<std::string>& overrides,
  const std::vector<std::string>& flavors,
  const std::string& document,
  const std::string& template_
) {
  if (!is_a_valid_absolute_path(root_path)) {
    spdlog::error("The root path '{}' isn't valid", root_path);
    return false;
  }

  if (!builder::is_a_valid_document_name(document)) {
    spdlog::error("The document '{}' don't have a valid name", document);
    return false;
  }

  if (!template_.empty() && (template_[0] != '_')) {
    spdlog::error("The template '{}' don't have a valid name", template_);
    return false;
  }

  absl::flat_hash_set<std::string> repeated_elements;

  for (size_t i = 0, l = overrides.size(); i < l; ++i) {
    if (!repeated_elements.insert(overrides[i]).second) {
      spdlog::error("The override '{}' is repeated", overrides[i]);
      return false;
    }
    if (!is_a_valid_relative_path(overrides[i])) {
      spdlog::error("The override '{}' isn't a valid relative path", overrides[i]);
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

} /* validator */
} /* mhconfig */
