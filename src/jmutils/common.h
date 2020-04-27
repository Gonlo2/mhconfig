#ifndef JMUTILS__COMMON_H
#define JMUTILS__COMMON_H

#include <random>

namespace jmutils
{

template <typename T>
struct zero_value_t {
  T v{0};
};

std::mt19937_64& prng_engine();

} /* jmutils */

#endif /* ifndef JMUTILS__COMMON_H */
