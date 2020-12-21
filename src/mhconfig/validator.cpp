#include "mhconfig/validator.h"

namespace mhconfig
{
namespace validator
{

bool are_valid_arguments(
  const std::string& root_path,
  const Labels& labels,
  const std::string& document
) {
  if (!is_a_valid_absolute_path(root_path)) {
    spdlog::error("The root path '{}' isn't valid", root_path);
    return false;
  }

  if (!is_a_valid_document_name(document)) {
    spdlog::error("The document '{}' don't have a valid name", document);
    return false;
  }

  auto b = labels.cbegin();
  if (b != labels.cend()) ++b;
  for (auto a = labels.cbegin(); b != labels.cend(); ++a, ++b) {
    if (a->first == b->first) {
      spdlog::error("The label '{}' is repeated", a->first);
      return false;
    }
  }

  return true;
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

    auto pos = path.find('/');
    if (pos == std::string::npos) {
      break;
    }
    path.remove_prefix(pos+1);
  }

  return true;
}

} /* validator */
} /* mhconfig */
