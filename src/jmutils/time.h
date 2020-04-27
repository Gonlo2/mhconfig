#ifndef JMUTILS__TIME_H
#define JMUTILS__TIME_H

#include <chrono>

namespace jmutils
{
namespace time
{

  uint64_t monotonic_now_sec();
  std::chrono::time_point<std::chrono::steady_clock> monotonic_now();

} /* time */
} /* jmutils */

#endif
