#ifndef JMUTILS__COMMON_H
#define JMUTILS__COMMON_H

#include <random>
#include <absl/synchronization/mutex.h>

namespace jmutils
{

template <typename T>
struct zero_value_t {
  T v{0};
};

std::mt19937_64& prng_engine();

inline bool is_little_endian() {
  uint32_t x = 1;
  uint8_t *c = (uint8_t*)&x;
  return *c;
}

inline void push_double(std::string& output, double n) {
  uint8_t *c = (uint8_t*)&n;
  if (is_little_endian()) {
    for (int i = 0; i < 8; ++i) {
      output.push_back(c[i]);
    }
  } else {
    for (int i = 0; i < 8; ++i) {
      output.push_back(c[7-i]);
    }
  }
}

inline void push_uint64(std::string& output, uint64_t n) {
  output.push_back(n & 0xff);
  output.push_back((n >> 8) & 0xff);
  output.push_back((n >> 16) & 0xff);
  output.push_back((n >> 24) & 0xff);
  output.push_back((n >> 32) & 0xff);
  output.push_back((n >> 40) & 0xff);
  output.push_back((n >> 48) & 0xff);
  output.push_back((n >> 56) & 0xff);
}

inline void push_uint32(std::string& output, uint32_t n) {
  output.push_back(n & 0xff);
  output.push_back((n >> 8) & 0xff);
  output.push_back((n >> 16) & 0xff);
  output.push_back((n >> 24) & 0xff);
}

inline void push_uint16(std::string& output, uint16_t n) {
  output.push_back(n & 0xff);
  output.push_back((n >> 8) & 0xff);
}

inline void push_varint(std::string& output, uint32_t n) {
  if (n < 0x80) {
    output.push_back(static_cast<uint8_t>(n));
  } else {
    output.push_back(static_cast<uint8_t>(128 | (n&127)));
    n >>= 7;
    if (n < 0x4000) {
      output.push_back(static_cast<uint8_t>(n));
    } else {
      output.push_back(static_cast<uint8_t>(128 | (n&127)));
      n >>= 7;
      if (n < 0x200000) {
        output.push_back(static_cast<uint8_t>(n&255));
      } else {
        output.push_back(static_cast<uint8_t>(128 | (n&127)));
        n >>= 7;
        output.push_back(static_cast<uint8_t>(n&255));
      }
    }
  }
}

template <typename T>
inline void push_str(std::string& output, T&& str) {
  push_varint(output, str.size());
  output += str;
}

template <typename T>
inline void swap_delete(T& container, size_t pos) {
  std::swap(container[pos], container.back());
  container.pop_back();
}

template <typename T>
inline size_t remove_expired_map(
  absl::Mutex& mtx,
  T& map
) {
  std::vector<typename T::key_type> to_remove;

  mtx.ReaderLock();
  size_t size = map.size();
  for (auto& it: map) {
    if (it.second.expired()) {
      to_remove.push_back(it.first);
    }
  }
  mtx.ReaderUnlock();

  if (!to_remove.empty()) {
    mtx.Lock();
    for (size_t i = 0, l = to_remove.size(); i < l; ++i) {
      if (auto search = map.find(to_remove[i]); search != map.end()) {
        if (search->second.expired()) {
          map.erase(search);
        }
      }
    }
    size = map.size();
    mtx.Unlock();
  }

  return size;
}

} /* jmutils */

#endif /* ifndef JMUTILS__COMMON_H */
