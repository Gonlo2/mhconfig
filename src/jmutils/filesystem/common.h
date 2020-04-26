#ifndef JMUTILS__FILESYSTEM__COMMON_H
#define JMUTILS__FILESYSTEM__COMMON_H

#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <unordered_set>

#include <boost/filesystem.hpp>

namespace jmutils
{
namespace filesystem
{

std::vector<std::string> recursive_list_files(
  const std::string& dir
);

std::string relative_parent_path(
  const std::string& child,
  const std::string& parent
);

std::pair<std::string, std::string> file_name_and_extension(
  const std::string& path
);

std::string join_paths(
  const std::string& a,
  const std::string& b
);

bool exists(
  const std::string& path
);

bool is_regular_file(
  const std::string& path
);

} /* filesystem */
} /* jmutils */

#endif /* ifndef JMUTILS__FILESYSTEM__COMMON_H */
