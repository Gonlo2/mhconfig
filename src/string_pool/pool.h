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

inline size_t align(size_t size) {
  return (size + 7) & ~7;
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
  result->hash = std::hash<std::string>{}(str);
  result->size = str.size();
  result->data = data;
  result->next = nullptr;
  result->chunk = nullptr;
  result->refcount.store(0);
}

string_t* alloc_string_ptr();
bool make_small_string(const std::string& str, uint64_t& result);
string_t* make_string_ptr(const std::string& str, chunk_t* chunk);

class String final
{
public:
  friend bool operator==(const String& lhs, const String& rhs);
  friend bool operator==(const String& lhs, const std::string& rhs);

  explicit String() noexcept {
    data_ = 0;
  }

  explicit String(const std::string& str) noexcept {
    if (!make_small_string(str, data_)) {
      void* data = alloc_string_ptr();
      assert (data != nullptr);
      string_t* ptr = new (data) string_t;
      init_string(str, (char*) str.c_str(), ptr);
      ptr->refcount.fetch_add(1, std::memory_order_relaxed);
      data_ = (uint64_t)ptr;
    }
  }

  String(uint64_t data) noexcept : data_(data) {
    if ((data_ & 1) == 0) {
      string_t* ptr = (string_t*) data_;
      if (ptr != nullptr) {
        ptr->refcount.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  String(const String& o) noexcept : data_(o.data_) {
    if ((data_ & 1) == 0) {
      string_t* ptr = (string_t*) data_;
      if (ptr != nullptr) {
        ptr->refcount.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  String(String&& o) noexcept : data_(o.data_) {
    o.data_ = 0;
  }

  String& operator=(const String& o) noexcept {
    data_ = o.data_;
    if ((data_ & 1) == 0) {
      string_t* ptr = (string_t*) data_;
      if (ptr != nullptr) {
        ptr->refcount.fetch_add(1, std::memory_order_relaxed);
      }
    }
    return *this;
  }

  String& operator=(String&& o) noexcept {
    data_ = o.data_;
    o.data_ = 0;
    return *this;
  }

  ~String() noexcept;

  size_t hash() const {
    if (data_ & 1) {
      return data_;
    } else {
      string_t* ptr = (string_t*) data_;
      return (ptr == nullptr) ? 0 : ptr->hash;
    }
  }

  size_t size() const {
    if (data_ & 1) {
      return (data_>>1) & 7;
    } else {
      string_t* ptr = (string_t*) data_;
      return (ptr == nullptr) ? 0 : ptr->size;
    }
  }

  std::string str() const {
    if (data_ & 1) {
      size_t size = (data_>>1) & 7;
      std::string result;
      uint64_t data = data_;
      while (size--) {
        data >>= 8;
        result.push_back(static_cast<int8_t>(data & 255));
      }
      return result;
    } else {
      string_t* ptr = (string_t*) data_;
      if (ptr == nullptr) return std::string();

      if (ptr->chunk != nullptr) {
        std::shared_lock lock(ptr->chunk->mutex);
        return std::string(ptr->data, ptr->size);
      }

      return std::string(ptr->data, ptr->size);
    }
  }

private:
  // This store also a pointer or a little string (up to 7 characters, this will
  // be enought for more that the half of the english words). To know what kind
  // of data it has we use the least significant bit, it has a zero in the case
  // of a pointer to string_t or a one in the case of a little string. This is
  // possible aligning the string_t data structures in even positions.
  //
  // The little string format follow the next schema
  // --------------------------------------------------------------------
  // | more significative           ...              less significative |
  // --------------------------------------------------------------------
  // |  ch7  |  ch6  |  ch5  |  ch4  |  ch3  |  ch2  |  ch1  | XXXXLLL1 |
  // --------------------------------------------------------------------
  // | 8bits | 8bits | 8bits | 8bits | 8bits | 8bits | 8bits |  8 bits  |
  // --------------------------------------------------------------------
  // Where
  // - chX is the X string character if exists and zero in other case
  // - X are unused bits
  // - LLL are the bits with the string size
  //
  //TODO check if make sense store up to 9 characters using only 6bits
  // by character, to do it use the next words
  // - the ascii words in upper and lower case (52 values)
  // - the ascii numbers (10 values)
  // - two extra characters, probaby the undescore and the dash
  uint64_t data_;
};

static_assert(sizeof(String) == 8, "The size of the String must be 8 bytes");

// Only it's supported if both string has the same format (small or ptr)
inline bool operator==(const String& lhs, const String& rhs) {
  if (lhs.data_ == rhs.data_) return true;
  if (lhs.data_ & 1) return false;

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
  if (lhs.data_ & 1) {
    if (((lhs.data_>>1)&7) != rhs.size()) return false;
    uint64_t l = lhs.data_;
    for (auto c: rhs) {
      l >>= 8;
      if (static_cast<int8_t>(l&255) != c) return false;
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
  uint32_t num_strings;
  uint32_t num_chunks;
  uint32_t reclaimed_bytes;
  uint32_t used_bytes;
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

class Pool
{
public:
  Pool(
    std::unique_ptr<StatsObserver>&& stats_observer
  )
    : stats_observer_(std::move(stats_observer))
  {
    head_ = new_chunk();
  }

  virtual ~Pool() {
    while (head_ != nullptr) {
      while (head_->first_string != nullptr) {
        head_->first_string->chunk = nullptr;
        head_->first_string = head_->first_string->next;
      }

      chunk_t* tmp = head_;
      head_ = tmp->next;
      tmp->~chunk_t();
      free(tmp);
    }

    stats_.num_strings = 0;
    stats_.num_chunks = 0;
    stats_.reclaimed_bytes = 0;
    stats_.used_bytes = 0;
    stats_observer_->on_updated_stats(stats_, true);
  }

  const String add(const std::string& str) {
    uint64_t result;
    if (!make_small_string(str, result)) {
      std::unique_lock lock(mutex_);

      auto search = set_.find(String(str));
      if (search != set_.end()) {
        return *search;
      }

      spdlog::debug("Adding a new string");
      return *set_.insert(store_string(str)).first;
    }

    return String(result);
  }

private:
  friend class String;

  std::unordered_set<String> set_;
  std::shared_mutex mutex_;
  chunk_t* head_;
  stats_t stats_;
  std::unique_ptr<StatsObserver> stats_observer_;

  const String store_string(const std::string& str) {
    spdlog::debug("Adding a new string of size {}", str.size());
    stats_.num_strings += 1;

    chunk_t* back_chunk = head_;
    chunk_t* chunk = head_;
    while (chunk != nullptr) {
      size_t s = (char*) chunk->next_data - (char*) &chunk->data;
      spdlog::trace("The chunk {} used {} bytes", (void*)chunk, s);
      if (s + str.size() <= CHUNK_DATA_SIZE) break;
      back_chunk = chunk;
      chunk = chunk->next;
    }
    if (chunk == nullptr) {
      chunk = back_chunk->next = new_chunk();
    }

    std::unique_lock lock_chunk(chunk->mutex);

    string_t* string = make_string_ptr(str, chunk);
    spdlog::trace("Made the string {} in {}", (void*)string, (void*)string->data);

    size_t data_size = align(str.size());
    spdlog::trace("Moving the next_data pointer {} bytes", data_size);
    chunk->next_data += data_size;
    stats_.used_bytes += data_size;

    stats_observer_->on_updated_stats(stats_, false);

    return String((uint64_t)string);
  }

  const void compact_chunk(chunk_t* chunk) {
    // First we block the accesses to the pool and the chunk to modify until the
    // compact process finish
    std::unique_lock lock_pool(mutex_);
    std::unique_lock lock_chunk(chunk->mutex);

    spdlog::debug("Compacting the chunk {}", (void*)chunk);

    stats_.used_bytes -= chunk->fragmented_size.exchange(0, std::memory_order_acq_rel);

    spdlog::trace("Looking for the first string");
    chunk->next_data = chunk->data;
    while (chunk->first_string != nullptr) {
      string_t* s = chunk->first_string;
      if (s->refcount.load(std::memory_order_acq_rel) != 1) break;
      spdlog::trace("Removing the string {}", (void*)s);
      chunk->first_string = s->next;
      // This is the hacky/tricky logic that avoid call recursively to compact
      // the same chunk after erase the new string, since we defined as null
      // the chunk the destructor will avoid the compacter branch and remove
      // successfully the string at the end :magic:
      s->chunk = nullptr;
      set_.erase(String((uint64_t)s));
      stats_.num_strings -= 1;
    }

    chunk->last_string = chunk->first_string;

    spdlog::trace("Compacting the strings");
    string_t* next_string = chunk->last_string;
    while (next_string != nullptr) {
      string_t* s = next_string;
      next_string = s->next;
      if (s->refcount.load(std::memory_order_acq_rel) == 1) {
        spdlog::trace("Removing the string {}", (void*)s);
        // The same as before ;)
        s->chunk = nullptr;
        set_.erase(String((uint64_t)s));
        stats_.num_strings -= 1;
      } else {
        spdlog::trace("Compacting the string {}", (void*)s);
        chunk->last_string->next = s;
        chunk->last_string = s;
        char* from = s->data;
        char* to = chunk->next_data;
        s->data = to;
        size_t data_size = align(s->size);
        chunk->next_data += data_size;
        // We compact the data in place \o/
        while (to != chunk->next_data) *to++ = *from++;
      }
    }

    if (chunk->last_string != nullptr) chunk->last_string->next = nullptr;

    stats_.used_bytes += chunk->next_data - chunk->data;

    stats_observer_->on_updated_stats(stats_, true);
  }

  chunk_t* new_chunk() {
    spdlog::trace("Making a new chunk");
    stats_.num_chunks += 1;
    stats_.reclaimed_bytes += CHUNK_DATA_SIZE;

    void* data = aligned_alloc(sizeof(size_t), sizeof(chunk_t));
    assert (data != nullptr);
    chunk_t* chunk = new (data) chunk_t;
    chunk->fragmented_size.store(0);
    chunk->next_data = (char*) &chunk->data;
    chunk->pool = this;
    chunk->first_string = nullptr;
    chunk->last_string = nullptr;
    chunk->next = nullptr;

    return chunk;
  }
};

} /* string_pool */

#endif /* ifndef STRING_POOL__POOL_H */
