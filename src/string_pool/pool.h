#ifndef STRING_POOL__POOL_H
#define STRING_POOL__POOL_H

#include <stdio.h>
#include <iostream>

#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>
#include <absl/hash/hash.h>

#include "spdlog/spdlog.h"

#define CHUNK_DATA_SIZE (1<<20)
#define CHUNK_STRING_SIZE (1<<15)

namespace string_pool
{

  namespace {
    inline size_t align(size_t size) {
      return (size + 7) & ~7;
    }

    const static char* CODED_VALUE_TO_ASCII_CHAR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    const static char ASCII_CHAR_TO_CODED_VALUE[] = {127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 63, 127, 127, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 127, 127, 127, 127, 127, 127, 127, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 127, 127, 127, 127, 62, 127, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127};

  }

  class Chunk;
  class String;

  class alignas(8) InternalString final
  {
  public:
    InternalString();

    InternalString(const InternalString& o) = delete;
    InternalString(InternalString&& o) = delete;

    InternalString& operator=(const InternalString& o) = delete;
    InternalString& operator=(InternalString&& o) = delete;

    inline size_t hash() const {
      return hash_;
    }

    inline uint32_t size() const {
      return size_;
    }

    inline std::string str() const;

  private:
    friend class Chunk;
    friend class String;
    friend String make_string(const std::string& str, InternalString* internal_string);
    friend bool operator==(const InternalString& lhs, const InternalString& rhs);
    friend bool operator==(const InternalString& lhs, const std::string& rhs);

    mutable std::atomic<int64_t> refcount_;
    const char* data_;
    Chunk* chunk_;
    size_t hash_;
    uint32_t size_;

    void init(const std::string& str, const char* data, Chunk* chunk);

    inline void increment_refcount() {
      refcount_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void decrement_refcount();
  };

  class String final
  {
  public:
    explicit String() noexcept;
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
      return ((InternalString*) data_)->hash();
    }

    inline size_t size() const {
      if (is_small() || (data_ == 0)) {
        return (data_ & 2)
          ? ((data_>>2) & 3) + 8
          : (data_>>2) & 7;
      }
      return ((InternalString*) data_)->size();
    }

    inline bool is_small() const {
      return data_ & 1;
    }

    std::string str() const;

    template <typename H>
    friend H AbslHashValue(H h, const String& s) {
      return H::combine(std::move(h), s.hash());
    }

  private:
    friend bool operator==(const String& lhs, const String& rhs);
    friend bool operator==(const String& lhs, const std::string& rhs);

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

}


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
  explicit Pool(std::unique_ptr<StatsObserver>&& stats_observer);

  ~Pool();

  Pool(const Pool& o) = delete;
  Pool(Pool&& o) = delete;

  Pool& operator=(const Pool& o) = delete;
  Pool& operator=(Pool&& o) = delete;

  const String add(const std::string& str);
  const stats_t& stats() const;
  void compact();

private:
  friend class Chunk;

  absl::flat_hash_set<String> set_;
  absl::Mutex mutex_;
  std::vector<Chunk*> chunks_;
  stats_t stats_;
  std::unique_ptr<StatsObserver> stats_observer_;

  const String store_string(const std::string& str);

  Chunk* new_chunk();
};


class Chunk final
{
private:
  friend class InternalString;
  friend class Pool;
  friend bool operator==(const InternalString& lhs, const InternalString& rhs);
  friend bool operator==(const InternalString& lhs, const std::string& rhs);

  mutable std::atomic<uint32_t> refcount_;
  std::atomic<int32_t> fragmented_size_;
  uint32_t next_data_;
  uint32_t next_string_in_use_;
  Pool* pool_;
  absl::Mutex mutex_;
  uint16_t strings_in_use_[CHUNK_STRING_SIZE];
  char data_[CHUNK_DATA_SIZE];
  InternalString strings_[CHUNK_STRING_SIZE];

  explicit Chunk(Pool* pool);
  ~Chunk();

  Chunk(const Chunk& o) = delete;
  Chunk(Chunk&& o) = delete;

  Chunk& operator=(const Chunk& o) = delete;
  Chunk& operator=(Chunk&& o) = delete;

  void remove_pool();

  inline void decrement_refcount() {
    if (refcount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      this->~Chunk();
      delete this;
    }
  }

  void released_string(uint32_t size);

  String add(const std::string& str, stats_t& stats);

  inline void compact(absl::flat_hash_set<String>& set, stats_t& stats) {
    mutex_.Lock();
    unsafe_compact(set, stats);
    mutex_.Unlock();
  }

  void unsafe_compact(absl::flat_hash_set<String>& set, stats_t& stats);

  void reallocate(InternalString* s);
};



inline bool operator==(const InternalString& lhs, const InternalString& rhs) {
  if (&lhs == &rhs) return true;
  if ((lhs.hash_ != rhs.hash_) || (lhs.size_ != rhs.size_)) return false;

  if (lhs.chunk_ == rhs.chunk_) {
    if (lhs.chunk_ != nullptr) return false;
    return memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
  } else if (lhs.chunk_ != nullptr) {
    bool result;
    lhs.chunk_->mutex_.ReaderLock();
    if (rhs.chunk_ != nullptr) {
      rhs.chunk_->mutex_.ReaderLock();
      result = memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
      rhs.chunk_->mutex_.ReaderUnlock();
    } else {
      result = memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
    }
    lhs.chunk_->mutex_.ReaderUnlock();
    return result;
  } else if (rhs.chunk_ != nullptr) {
    rhs.chunk_->mutex_.ReaderLock();
    bool result = memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
    rhs.chunk_->mutex_.ReaderUnlock();
    return result;
  }

  return memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
}

inline bool operator!=(const InternalString& lhs, const InternalString& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const InternalString& lhs, const std::string& rhs) {
  if (lhs.size_ != rhs.size()) return false;

  if (lhs.chunk_ != nullptr) {
    lhs.chunk_->mutex_.ReaderLock();
    bool result = memcmp(lhs.data_, rhs.c_str(), lhs.size_) == 0;
    lhs.chunk_->mutex_.ReaderUnlock();
    return result;
  }

  return memcmp(lhs.data_, rhs.c_str(), lhs.size_) == 0;
}

inline bool operator!=(const InternalString& lhs, const std::string& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const String& lhs, const String& rhs) {
  if (lhs.data_ == rhs.data_) return true;
  if (lhs.is_small() || rhs.is_small()) return false;

  if ((lhs.data_ == 0) || (rhs.data_ == 0)) return false;
  return *((InternalString*) lhs.data_) == *((InternalString*) rhs.data_);
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
      for (size_t i = 0, end = rhs.size(); i < end; ++i) {
        if (CODED_VALUE_TO_ASCII_CHAR[data & 63] != rhs[i]) return false;
        data >>= 6;
      }
    } else {
      if (((data>>2) & 7) != rhs.size()) return false;
      for (size_t i = 0, end = rhs.size(); i < end; ++i) {
        data >>= 8;
        if (static_cast<char>(data & 255) != rhs[i]) return false;
      }
    }
    return true;
  }

  return *((InternalString*) lhs.data_) == rhs;
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


bool make_small_string(const std::string& str, uint64_t& result);

String make_small_string(const std::string& str);
String make_string(const std::string& str, InternalString* internal_string);

} /* string_pool */

#endif /* ifndef STRING_POOL__POOL_H */
