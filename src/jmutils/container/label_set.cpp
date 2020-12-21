#include "jmutils/container/label_set.h"

namespace jmutils
{
namespace container
{

Labels make_labels(
  std::vector<label_t>&& labels
) {
  std::sort(labels.begin(), labels.end());
  return Labels(labels);
}

std::string Labels::repr() const {
  std::stringstream ss;
  ss << "(";
  auto it = labels_.cbegin();
  if (it != labels_.cend()) {
    ss << "'" << it->first << "': '" << it->second << "'";
    ++it;
  }
  while (it != labels_.cend()) {
    ss << ", '" << it->first << "': '" << it->second << "'";
    ++it;
  }
  ss << ")";
  return ss.str();
}


} /* container */
} /* jmutils */
