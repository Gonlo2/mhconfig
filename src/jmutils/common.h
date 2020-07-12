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

inline void push_uint32(std::string& output, uint32_t n) {
  output.push_back((n >> 24) & 0xff);
  output.push_back((n >> 16) & 0xff);
  output.push_back((n >> 8) & 0xff);
  output.push_back(n & 0xff);
}

inline void push_str(std::string& output, const std::string& str) {
  size_t l = str.size();
  if (l < 0x80) {
    output.push_back(static_cast<uint8_t>(l));
  } else {
    output.push_back(static_cast<uint8_t>(128 | (l&127)));
    l >>= 7;
    if (l < 0x4000) {
      output.push_back(static_cast<uint8_t>(l));
    } else {
      output.push_back(static_cast<uint8_t>(128 | (l&127)));
      l >>= 7;
      if (l < 0x200000) {
        output.push_back(static_cast<uint8_t>(l&255));
      } else {
        output.push_back(static_cast<uint8_t>(128 | (l&127)));
        l >>= 7;
        output.push_back(static_cast<uint8_t>(l&255));
      }
    }
  }
  output += str;
}

template <typename T>
inline void swap_delete(T& container, size_t pos) {
  std::swap(container[pos], container.back());
  container.pop_back();
}

} /* jmutils */

#endif /* ifndef JMUTILS__COMMON_H */
