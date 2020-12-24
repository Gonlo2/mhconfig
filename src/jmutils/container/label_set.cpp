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

} /* container */
} /* jmutils */
