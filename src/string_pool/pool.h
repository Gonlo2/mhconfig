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
  string_t* first_string;
  string_t* last_string;
  Pool* pool;
  chunk_t* next;
  std::shared_mutex mutex;
  char data[CHUNK_DATA_SIZE];
};

struct string_t {
  uint32_t hash;
  uint32_t size : 31;
  bool needs_to_be_destroyed : 1;
  char* data;
  chunk_t* chunk;
  string_t* next;
  mutable std::atomic<uint64_t> refcount;
};


inline void init_string(
  const std::string& str,
  char* data,
  string_t* result,
  bool needs_to_be_destroyed
) {
  result->hash = std::hash<std::string>{}(str);
  result->size = str.size();
  result->needs_to_be_destroyed = needs_to_be_destroyed;
  result->data = data;
  result->next = nullptr;
  result->chunk = nullptr;
  result->refcount.store(0);
}

string_t* make_string_ptr(const std::string& str, chunk_t* chunk);

class String
{
public:
  friend bool operator==(const String& lhs, const String& rhs);
  friend bool operator==(const String& lhs, const std::string& rhs);

  explicit String() noexcept : ptr_(nullptr) {
  }

  //TODO avoid create data
  explicit String(const std::string& str) noexcept {
    void* data = aligned_alloc(sizeof(size_t), sizeof(string_t));
    assert (data != nullptr);
    ptr_ = new (data) string_t;
    init_string(str, (char*) str.c_str(), ptr_, true);
    ptr_->refcount.fetch_add(1, std::memory_order_relaxed);
  }

  //TODO avoid create data
  explicit String(const std::string& str, string_t* internal_struct) noexcept
    : ptr_(internal_struct)
  {
    init_string(str, (char*) str.c_str(), ptr_, false);
  }

  String(string_t* ptr) noexcept : ptr_(ptr) {
    if ((ptr_ != nullptr) && ptr_->needs_to_be_destroyed) {
      ptr_->refcount.fetch_add(1, std::memory_order_relaxed);
    }
  }

  String(const String& o) noexcept : ptr_(o.ptr_) {
    if ((ptr_ != nullptr) && ptr_->needs_to_be_destroyed) {
      ptr_->refcount.fetch_add(1, std::memory_order_relaxed);
    }
  }

  String(String&& o) noexcept : ptr_(o.ptr_) {
    o.ptr_ = nullptr;
  }

  String& operator=(const String& o) noexcept {
    ptr_ = o.ptr_;
    if ((ptr_ != nullptr) && ptr_->needs_to_be_destroyed) {
      ptr_->refcount.fetch_add(1, std::memory_order_relaxed);
    }
    return *this;
  }

  String& operator=(String&& o) noexcept {
    ptr_ = o.ptr_;
    o.ptr_ = nullptr;
    return *this;
  }

  virtual ~String() noexcept;

  size_t hash() const {
    return (ptr_ == nullptr) ? 0 : ptr_->hash;
  }

  size_t size() const {
    return (ptr_ == nullptr) ? 0 : ptr_->size;
  }

  std::string str() const {
    if (ptr_ == nullptr) return std::string();

    if (ptr_->chunk != nullptr) {
      std::shared_lock lock(ptr_->chunk->mutex);
      return std::string(ptr_->data, ptr_->size);
    }

    return std::string(ptr_->data, ptr_->size);
  }

private:
  string_t* ptr_;
};

inline bool operator==(const String& lhs, const String& rhs) {
  if (lhs.ptr_ == rhs.ptr_) return true;
  if ((lhs.ptr_ == nullptr) || (rhs.ptr_ == nullptr)) return false;
  if ((lhs.ptr_->hash != rhs.ptr_->hash) || (lhs.ptr_->size != rhs.ptr_->size)) return false;
  return memcmp(lhs.ptr_->data, rhs.ptr_->data, lhs.ptr_->size) == 0;
}

inline bool operator!=(const String& lhs, const String& rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const String& lhs, const std::string& rhs) {
  if (lhs.ptr_ == nullptr) return false;
  if (lhs.ptr_->size != rhs.size()) return false;
  return memcmp(lhs.ptr_->data, rhs.c_str(), lhs.ptr_->size) == 0;
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
  struct hash<string_pool::String> {
    std::size_t operator()(const string_pool::String& v) const {
      return v.hash();
    }
  };
}


namespace string_pool
{

class Pool
{
public:
  Pool() {
    chunk_ = new_chunk();
  }

  virtual ~Pool() {
    while (chunk_ != nullptr) {
      while (chunk_->first_string != nullptr) {
        chunk_->first_string->chunk = nullptr;
        chunk_->first_string = chunk_->first_string->next;
      }

      chunk_t* tmp = chunk_;
      chunk_ = tmp->next;
      tmp->~chunk_t();
      free(tmp);
    }
  }

  const String add(const std::string& str) {
    std::unique_lock lock(chunk_->mutex);

    auto search = set_.find(String(str));
    if (search != set_.end()) return *search;

    spdlog::debug("Adding a new string");
    return *set_.insert(store_string(str)).first;
  }

private:
  friend class String;

  std::unordered_set<String> set_;
  chunk_t* chunk_;

  const String store_string(const std::string& str) {
    spdlog::debug("Adding a new string of size {}", str.size());

    chunk_t* back_chunk = chunk_;
    while (back_chunk->next != nullptr) {
      size_t s = (char*) back_chunk->next->next_data - (char*) &back_chunk->next->data;
      spdlog::trace("The chunk {} used {} bytes", (uint64_t)back_chunk->next, s);
      if (s + str.size() <= CHUNK_DATA_SIZE) break;
      back_chunk = back_chunk->next;
    }

    if (back_chunk->next == nullptr) back_chunk->next = new_chunk();

    std::unique_lock lock_chunk(back_chunk->next->mutex);

    string_t* string = make_string_ptr(str, back_chunk->next);
    spdlog::trace("Made the string {} in {}", (void*)string, (void*)string->data);

    size_t data_size = align(str.size());
    spdlog::trace("Moving the next_data pointer {} bytes", data_size);
    back_chunk->next->next_data += data_size;

    return String(string);
  }

  const void compact_chunk(chunk_t* chunk) {
    std::unique_lock lock_pool(chunk_->mutex);
    std::unique_lock lock_chunk(chunk->mutex);

    spdlog::debug("Compacting the chunk {}", (void*)chunk);

    chunk->fragmented_size.store(0, std::memory_order_relaxed);

    spdlog::trace("Looking for the first string");
    chunk->next_data = chunk->data;
    while (chunk->first_string != nullptr) {
      string_t* s = chunk->first_string;
      if (s->refcount.load(std::memory_order_relaxed) != 1) break;
      spdlog::trace("Removing the string {}", (void*)s);
      chunk->first_string = s->next;
      s->chunk = nullptr;
      set_.erase(String(s));
    }

    chunk->last_string = chunk->first_string;

    spdlog::trace("Compacting the strings");
    string_t* next_string = chunk->last_string;
    while (next_string != nullptr) {
      string_t* s = next_string;
      next_string = s->next;
      if (s->refcount.load(std::memory_order_relaxed) == 1) {
        spdlog::trace("Removing the string {}", (void*)s);
        s->chunk = nullptr;
        set_.erase(String(s));
      } else {
        spdlog::trace("Compacting the string {}", (void*)s);
        chunk->last_string->next = s;
        chunk->last_string = s;
        char* from = s->data;
        char* to = chunk->next_data;
        s->data = to;
        size_t data_size = align(s->size);
        chunk->next_data += data_size;
        while (to != chunk->next_data) *to++ = *from++;
      }
    }

    if (chunk->last_string != nullptr) chunk->last_string->next = nullptr;
  }

  chunk_t* new_chunk() {
    spdlog::trace("Making a new chunk");
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
