#include "jmutils/common.h"

namespace jmutils
{

std::mt19937_64& prng_engine() {
  thread_local static std::mt19937_64 engine{std::random_device{}()};
  return engine;
}

} /* jmutils */
