#ifndef JMUTILS__COMMON_H
#define JMUTILS__COMMON_H

#include <random>

namespace jmutils
{

namespace {
  union uint32_converter_t {
    char c[4];
    uint32_t n;
  };
}

template <typename T>
struct zero_value_t {
  T v{0};
};

std::mt19937_64& prng_engine();

inline void push_uint32(std::string& output, uint32_t n) {
  uint32_converter_t uint32_converter;
  uint32_converter.n = n;
  output += uint32_converter.c;
}

inline void push_str(std::string& output, const std::string& str) {
  push_uint32(output, str.size());
  output += str;
}

template <typename T>
inline void swap_delete(T& container, size_t pos) {
  std::swap(container[pos], container.back());
  container.pop_back();
}

} /* jmutils */

#endif /* ifndef JMUTILS__COMMON_H */
