#include "jmutils/time.h"

namespace jmutils
{
namespace time
{

  uint64_t monotonic_now_sec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now().time_since_epoch()
    ).count();
  }

  MonotonicTimePoint monotonic_now() {
    return std::chrono::steady_clock::now();
  }

} /* time */
} /* jmutils */
