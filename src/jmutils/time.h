#ifndef JMUTILS__TIME_H
#define JMUTILS__TIME_H

#include <chrono>

namespace jmutils
{

typedef std::chrono::time_point<std::chrono::steady_clock> MonotonicTimePoint;

uint64_t now_sec();

uint64_t monotonic_now_sec();
uint64_t monotonic_now_ms();
MonotonicTimePoint monotonic_now();

} /* jmutils */

#endif
