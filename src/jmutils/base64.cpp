#include "jmutils/base64.h"

namespace jmutils
{

namespace base64 {
  static constexpr uint8_t decoding_table[] = {
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
    64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
    64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
  };
}

bool base64_sanitize(std::string& value) {
  size_t sanitized_size = 0;
  size_t num_equals = 0;
  for (size_t i = 0, l = value.size(); i < l; ++i) {
    if ((value[i] == '=') || (base64::decoding_table[static_cast<size_t>(value[i])] != 64)) {
      if (value[i] == '=') ++num_equals;
      if (sanitized_size != i) value[sanitized_size] = value[i];
      ++sanitized_size;
    } else {
      if ((value[i] != '\n') && (value[i] != ' ')) return false;
    }
  }

  switch (sanitized_size & 3) {
    case 3:
      sanitized_size += 1;
      value.resize(sanitized_size);
      value[sanitized_size-1] = '=';
      num_equals += 1;
      break;
    case 2:
      sanitized_size += 2;
      value.resize(sanitized_size);
      value[sanitized_size-2] = '=';
      value[sanitized_size-1] = '=';
      num_equals += 2;
      break;
    case 1:
      value.resize(sanitized_size);
      return false;
    case 0:
      value.resize(sanitized_size);
      break;
  }

  size_t expected_num_equals = 0;
  if (sanitized_size) {
    if (value[sanitized_size-1] == '=') ++expected_num_equals;
    if (value[sanitized_size-2] == '=') ++expected_num_equals;
  }

  return expected_num_equals == num_equals;
}

// This call asume the only valid characters are in the input and that it
// have a size multiple of four
void base64_decode(const std::string& input, std::string& out) {
  size_t l = input.size();

  out.clear();
  out.reserve(3 * (l >> 2));

  for (size_t i = 4; i < l; i += 4) {
    uint8_t a = base64::decoding_table[static_cast<size_t>(input[i-4])];
    uint8_t b = base64::decoding_table[static_cast<size_t>(input[i-3])];
    uint8_t c = base64::decoding_table[static_cast<size_t>(input[i-2])];
    uint8_t d = base64::decoding_table[static_cast<size_t>(input[i-1])];

    out.push_back((a << 2) | (b >> 4));
    out.push_back((b << 4) | (c >> 2));
    out.push_back((c << 6) | d);
  }

  uint8_t a = base64::decoding_table[static_cast<size_t>(input[l-4])];
  uint8_t b = base64::decoding_table[static_cast<size_t>(input[l-3])];
  uint8_t c = base64::decoding_table[static_cast<size_t>(input[l-2])];
  uint8_t d = base64::decoding_table[static_cast<size_t>(input[l-1])];

  out.push_back((a << 2) | (b >> 4));

  if (c != 64) {
    out.push_back((b << 4) | (c >> 2));
    if (d != 64) {
      out.push_back((c << 6) | d);
    }
  }
}

} /* jmutils */
