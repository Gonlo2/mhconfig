#ifndef STRING_POOL__POOL_H
#define STRING_POOL__POOL_H

#include <stdio.h>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <iostream>

#include "spdlog/spdlog.h"

#define CHUNK_DATA_SIZE (1<<22)

namespace string_pool
{

namespace {
  inline size_t align(size_t size) {
    return (size + 7) & ~7;
  }

  const static char* CODED_VALUE_TO_ASCII_CHAR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
  const static char ASCII_CHAR_TO_CODED_VALUE[] = {127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 63, 127, 127, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 127, 127, 127, 127, 127, 127, 127, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 127, 127, 127, 127, 62, 127, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127};
}

struct string_t;
class Pool;

struct chunk_t {
  std::atomic<uint32_t> fragmented_size;
  char* next_data;
  // We could life only with one pointer to the next string but with the
  // last string we could link the new strings in the same order that the
  // data sections, allowing compact the strings in place
  string_t* first_string;
  string_t* last_string;
  Pool* pool;
  chunk_t* next;
  std::shared_mutex mutex;
  char data[CHUNK_DATA_SIZE];
};

struct string_t {
  mutable std::atomic<uint64_t> refcount;
  char* data;
  chunk_t* chunk;
  string_t* next;
  size_t hash;
  uint32_t size;
};

inline void init_string(
  const std::string& str,
  char* data,
  string_t* result
) {
  result->refcount.store(0);
  result->data = data;
  result->chunk = nullptr;
  result->next = nullptr;
  result->hash = std::hash<std::string>{}(str);
  result->size = str.size();
}

string_t* alloc_string_ptr();
bool make_small_string(const std::string& str, uint64_t& result);
string_t* make_string_ptr(const std::string& str, chunk_t* chunk);

class String final
{
public:
  friend bool operator==(const String& lhs, const String& rhs);
  friend bool operator==(const String& lhs, const std::string& rhs);

  explicit String() noexcept;
  explicit String(const std::string& str) noexcept;
  explicit String(uint64_t data) noexcept;

  String(const String& o) noexcept;
  String(String&& o) noexcept;

  String& operator=(const String& o) noexcept;
  String& operator=(String&& o) noexcept;

  ~String() noexcept;

  inline size_t hash() const {
    if (is_small() || (data_ == 0)) {
      return data_;
    }
    return ((string_t*) data_)->hash;
  }

  inline size_t size() const {
    if (is_small() || (data_ == 0)) {
      return (data_ & 2)
        ? ((data_>>2) & 3) + 8
        : (data_>>2) & 7;
    }
    return ((string_t*) data_)->size;
  }

  std::string str() const;

  inline bool is_small() const {
    return data_ & 1;
  }

private:
  // This store also a pointer or a little string (up to 7 characters, this will
  // be enought for more that the half of the english words). To know what kind
  // of data it has we use the least significant bit, it has a zero in the case
  // of a pointer to string_t or a one in the case of a little string. This is
  // possible aligning the string_t data structures in even positions.
  //
  // The little string format follow the next schema if it has [0,8) characters
  // ---------------------------------------------------------------------------
  // | most significative             64 bits               less significative |
  // ---------------------------------------------------------------------------
  // | GGGGGGGG FFFFFFFF EEEEEEEE DDDDDDDD CCCCCCCC BBBBBBBB AAAAAAAA ***LLL01 |
  // ---------------------------------------------------------------------------
  // Where
  // - A, B, C, D, E, F, G are the string character if exists or zero in other case
  // - * are unused bits
  // - LLL are the bits with the string size
  // - 01 indicate that the data contain a small string with [0,8) characters
  //
  // Also exists a small+ string format with the next schema if it has [0,11)
  // characters and some special restrictions are met
  // ---------------------------------------------------------------------------
  // | most significative             64 bits               less significative |
  // ---------------------------------------------------------------------------
  // | JJJJJJII IIIIHHHH HHGGGGGG FFFFFFEE EEEEDDDD DDCCCCCC BBBBBBAA AAAALL11 |
  // ---------------------------------------------------------------------------
  // Where
  // - A, B, C, D, E, F, G, H, I, J are the coded string character (6 bits)
  // if exists or zero in other case
  // - LL are the bits with the string size minus eight (the size 11 is impossible)
  // - 11 indicate that the data contain a small+ string with [0,11) characters
  //
  // The coded string include the ascii words [a,z], [A,Z], [0,9], undescore and dash
  uint64_t data_;
};

static_assert(sizeof(String) == 8, "The size of the String must be 8 bytes");

// Only it's supported if both string has the same format (small or ptr)
inline bool operator==(const String& lhs, const String& rhs) {
  if (lhs.data_ == rhs.data_) return true;
  if (lhs.is_small() || rhs.is_small()) return false;

  string_t* l = (string_t*) lhs.data_;
  string_t* r = (string_t*) rhs.data_;
  if ((l == nullptr) || (r == nullptr)) return false;
  if ((l->hash != r->hash) || (l->size != r->size)) return false;

  if (l->chunk == r->chunk) {
    if (l->chunk != nullptr) {
      std::shared_lock lock(l->chunk->mutex);
      return memcmp(l->data, r->data, l->size) == 0;
    }
    return memcmp(l->data, r->data, l->size) == 0;
  } else if (l->chunk != nullptr) {
    std::shared_lock llock(l->chunk->mutex);
    if (r->chunk != nullptr) {
      std::shared_lock rlock(r->chunk->mutex);
      return memcmp(l->data, r->data, l->size) == 0;
    }
    return memcmp(l->data, r->data, l->size) == 0;
  } else if (r->chunk != nullptr) {
    std::shared_lock rlock(r->chunk->mutex);
    return memcmp(l->data, r->data, l->size) == 0;
  }

  return memcmp(l->data, r->data, l->size) == 0;
}


inline bool operator!=(const String& lhs, const String& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const String& lhs, const std::string& rhs) {
  if (lhs.data_ == 0) return false;
  if (lhs.is_small()) {
    uint64_t data = lhs.data_;
    if (data & 2) {
      if ((((data>>2) & 3) + 8) != rhs.size()) return false;
      data >>= 4;
      for (char c: rhs) {
        if (CODED_VALUE_TO_ASCII_CHAR[data & 63] != c) return false;
        data >>= 6;
      }
    } else {
      if (((data>>2) & 7) != rhs.size()) return false;
      for (char c: rhs) {
        data >>= 8;
        if (static_cast<char>(data & 255) != c) return false;
      }
    }
    return true;
  } else {
    string_t* l = (string_t*) lhs.data_;
    if (l->size != rhs.size()) return false;

    if (l->chunk != nullptr) {
      std::shared_lock rlock(l->chunk->mutex);
      return memcmp(l->data, rhs.c_str(), l->size) == 0;
    }

    return memcmp(l->data, rhs.c_str(), l->size) == 0;
  }
}

inline bool operator!=(const String& lhs, const std::string& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const std::string& lhs, const String& rhs) {
  return !(rhs == lhs);
}

inline bool operator!=(const std::string& lhs, const String& rhs) {
  return !(rhs == lhs);
}

} /* string_pool */


namespace std {
  template <>
  struct hash<::string_pool::String> {
    std::size_t operator()(const ::string_pool::String& v) const {
      return v.hash();
    }
  };
}


namespace string_pool
{

struct stats_t {
  uint32_t num_strings{0};
  uint32_t num_chunks{0};
  uint32_t reclaimed_bytes{0};
  uint32_t used_bytes{0};
};

class StatsObserver
{
public:
  StatsObserver() {
  }
  virtual ~StatsObserver() {
  }

  virtual void on_updated_stats(const stats_t& stats, bool force) {
  }
};

class Pool final
{
public:
  Pool();
  Pool(std::unique_ptr<StatsObserver>&& stats_observer);

  ~Pool();

  Pool(const Pool& o) = delete;
  Pool(Pool&& o) = delete;

  Pool& operator=(const Pool& o) = delete;
  Pool& operator=(Pool&& o) = delete;

  const String add(const std::string& str);
  const stats_t& stats() const;
  void compact();

private:
  friend class String;

  std::unordered_set<String> set_;
  std::shared_mutex mutex_;
  chunk_t* head_;
  stats_t stats_;
  std::unique_ptr<StatsObserver> stats_observer_;

  const String store_string(const std::string& str);

  void compact_chunk(chunk_t* chunk);
  void remove_string(string_t* s);
  void reallocate_string(chunk_t* chunk, string_t* s);

  chunk_t* new_chunk();
};

} /* string_pool */

#endif /* ifndef STRING_POOL__POOL_H */
