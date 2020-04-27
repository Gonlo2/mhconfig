#ifndef JMUTILS__TIME_H
#define JMUTILS__TIME_H

#include <chrono>

namespace jmutils
{
namespace time
{

  typedef std::chrono::time_point<std::chrono::steady_clock> MonotonicTimePoint;

  uint64_t monotonic_now_sec();
  MonotonicTimePoint monotonic_now();

} /* time */
} /* jmutils */

#endif
