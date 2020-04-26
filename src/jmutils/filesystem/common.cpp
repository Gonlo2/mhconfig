#include "jmutils/filesystem/common.h"

namespace jmutils
{
namespace filesystem
{

std::vector<std::string> recursive_list_files(
  const std::string& dir
) {
  std::vector<std::string> files;
  for (auto it : boost::filesystem::recursive_directory_iterator(dir)) {
    if (boost::filesystem::is_regular_file(it.path())) {
      files.push_back(it.path().string());
    }
  }

  return files;
}

std::string relative_parent_path(
  const std::string& child,
  const std::string& parent
) {
  return boost::filesystem::relative(child, parent).parent_path().string();
}

std::pair<std::string, std::string> file_name_and_extension(
  const std::string& path
) {
  boost::filesystem::path path_object(path);

  return std::make_pair(
    path_object.stem().string(),
    path_object.extension().string()
  );
}

std::string join_paths(
  const std::string& a,
  const std::string& b
) {
  boost::filesystem::path a_path(a);
  boost::filesystem::path b_path(b);
  return (a_path / b_path).string();
}

bool exists(
  const std::string& path
) {
  return boost::filesystem::exists(path);
}

bool is_regular_file(
  const std::string& path
) {
  return boost::filesystem::is_regular_file(path);
}

} /* filesystem */
} /* jmutils */
