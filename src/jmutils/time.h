#ifndef JMUTILS__TIME_H
#define JMUTILS__TIME_H

#include <chrono>

namespace jmutils
{
namespace time
{

  uint64_t monotonic_now_sec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now().time_since_epoch()
    ).count();
  };

} /* time */
} /* jmutils */

#endif
