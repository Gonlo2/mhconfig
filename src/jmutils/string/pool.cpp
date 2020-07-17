#include "jmutils/string/pool.h"

namespace jmutils
{
namespace string
{

InternalString::InternalString() {
  refcount_.store(0);
  data_ = nullptr;
  chunk_ = nullptr;
  hash_ = 0;
  size_ = 0;
}

inline std::string InternalString::str() const {
  if (chunk_ != nullptr) {
    chunk_->mutex_.ReaderLock();
    auto result = std::string(data_, size_);
    chunk_->mutex_.ReaderUnlock();
    return result;
  }
  return std::string(data_, size_);
}

void InternalString::init(const std::string& str, const char* data, Chunk* chunk) {
  refcount_.store(0);
  data_ = data;
  chunk_ = chunk;
  hash_ = std::hash<std::string>{}(str);
  size_ = str.size();
}

inline void InternalString::decrement_refcount() {
  if (chunk_ != nullptr) {
    switch (refcount_.fetch_sub(1, std::memory_order_acq_rel)) {
      case 1:
        spdlog::trace("This is the last string, decrementing the chunk refcount");
        chunk_->decrement_refcount();
        break;
      case 2:
        spdlog::trace("The only string left is the one in the set, marking the space to release");
        chunk_->released_string(size_);
        break;
    }
  }
}


String::String() noexcept {
  data_ = 0;
}

String::String(uint64_t data) noexcept : data_(data) {
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->increment_refcount();
  }
}

String::String(const String& o) noexcept : data_(o.data_) {
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->increment_refcount();
  }
}

String::String(String&& o) noexcept : data_(std::exchange(o.data_, 0)) {
}

String& String::operator=(const String& o) noexcept {
  if(&o == this) return *this;
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->decrement_refcount();
  }
  data_ = o.data_;
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->increment_refcount();
  }
  return *this;
}

String& String::operator=(String&& o) noexcept {
  if (this != &o) {
    std::swap(data_, o.data_);
  }
  return *this;
}

String::~String() noexcept {
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->decrement_refcount();
    data_ = 0;
  }
}

std::string String::str() const {
  if (is_small() || (data_ == 0)) {
    std::string result;
    result.reserve(10);
    uint64_t data = data_;
    if (data_ & 2) {
      data >>= 4;
      switch ((data_>>2) & 3) {
        case 2:
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          // fallthrough
        case 1:
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          // fallthrough
        case 0:
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          break;
      }
    } else {
      switch ((data_>>2) & 7) {
        case 7:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 6:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 5:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 4:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 3:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 2:
          data >>= 8;
          result += static_cast<char>(data & 255);
          // fallthrough
        case 1:
          data >>= 8;
          result += static_cast<char>(data & 255);
          break;
      }
    }
    return result;
  }

  return ((InternalString*) data_)->str();
}


Pool::Pool()
  : stats_observer_(nullptr),
  pool_context_(std::make_shared<pool_context_t>())
{
}

Pool::Pool(
  std::unique_ptr<StatsObserver>&& stats_observer
)
  : stats_observer_(std::move(stats_observer)),
  pool_context_(std::make_shared<pool_context_t>())
{
}

Pool::~Pool() {
  pool_context_->mutex.Lock();
  pool_context_->active = false;

  pool_context_->stats.num_strings = 0;
  pool_context_->stats.num_chunks = 0;
  pool_context_->stats.reclaimed_bytes = 0;
  pool_context_->stats.used_bytes = 0;

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(pool_context_->stats, true);
  }

  pool_context_->set.clear();
  pool_context_->mutex.Unlock();

  for (Chunk* chunk : chunks_) {
    chunk->decrement_refcount();
  }
}

const String Pool::add(const std::string& str) {
  InternalString internal_string;
  String s = make_string(str, &internal_string);
  if (!s.is_small()) {
    //TODO Rethink the string build part to avoid create the str and
    //change the unique mutex with a shared one to allow parallel
    //calls if the string already exists in the set
    pool_context_->mutex.Lock();

    auto search = pool_context_->set.find(s);
    if (search != pool_context_->set.end()) {
      String result = *search;
      pool_context_->mutex.Unlock();
      return result;
    }

    spdlog::trace("Adding a new string");
    auto result = store_string(str);
    pool_context_->set.insert(result);
    pool_context_->mutex.Unlock();
    return result;
  }

  return s;
}

const stats_t& Pool::stats() const {
  return pool_context_->stats;
}

void Pool::compact() {
  spdlog::trace("Compacting the chunks");

  pool_context_->mutex.Lock();
  for (Chunk* chunk : chunks_) {
    chunk->compact();
  }
  pool_context_->mutex.Unlock();
}

const String Pool::store_string(const std::string& str) {
  spdlog::trace("Adding a new string of size {}", str.size());
  pool_context_->stats.num_strings += 1;

  size_t idx = 0;
  size_t end = chunks_.size();
  while (
    (idx != end)
    && (
      (chunks_[idx]->next_string_in_use_ == CHUNK_STRING_SIZE)
      || (chunks_[idx]->next_data_ + str.size() >= CHUNK_DATA_SIZE)
    )
  ) {
    ++idx;
  }
  if (idx == end) chunks_.push_back(new_chunk());

  auto result = chunks_[idx]->add(str);

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(pool_context_->stats, false);
  }

  return result;
}

Chunk* Pool::new_chunk() {
  spdlog::trace("Making a new chunk");
  pool_context_->stats.num_chunks += 1;
  pool_context_->stats.reclaimed_bytes += CHUNK_DATA_SIZE;

  void* data = aligned_alloc(8, sizeof(Chunk));
  assert (data != nullptr);
  return new (data) Chunk(pool_context_);
}


Chunk::Chunk(std::shared_ptr<pool_context_t> pool_context) : pool_context_(std::move(pool_context)) {
  refcount_.store(1);
  fragmented_size_.store(0);
  next_data_ = 0;
  next_string_in_use_ = 0;

  for (size_t i = 0; i < CHUNK_STRING_SIZE; ++i) {
    strings_in_use_[i] = i;
  }
}

Chunk::~Chunk() {
}

void Chunk::released_string(uint32_t size) {
  ssize_t fragmented_size = fragmented_size_.fetch_add(
    size,
    std::memory_order_relaxed
  ) + size;

  spdlog::trace(
    "Checking if it's possible compact the chunk (fragmented_size: {}, limit: {})",
    fragmented_size,
    CHUNK_DATA_SIZE>>1
  );
  if (fragmented_size > (CHUNK_DATA_SIZE>>1)) {
    if (fragmented_size_.load(std::memory_order_acq_rel) > (CHUNK_DATA_SIZE>>1)) {
      pool_context_->mutex.Lock();
      if (pool_context_->active && (fragmented_size_.load(std::memory_order_acq_rel) > (CHUNK_DATA_SIZE>>1))) {
        compact();
      }
      pool_context_->mutex.Unlock();
    }
  }
}

String Chunk::add(const std::string& str) {
  mutex_.Lock();

  refcount_.fetch_add(1, std::memory_order_relaxed);

  spdlog::trace("Adding a string of size {} in {}", str.size(), (void*)this);
  strings_[next_string_in_use_].init(str, &data_[next_data_], this);
  memcpy(&data_[next_data_], str.c_str(), str.size());

  spdlog::trace("Moving the next_data pointer {} bytes", str.size());
  next_data_ += str.size();
  pool_context_->stats.used_bytes += str.size();
  String result((uint64_t)&strings_[next_string_in_use_++]);
  mutex_.Unlock();

  return result;
}

void Chunk::compact() {
  mutex_.Lock();

  spdlog::trace("Compacting the chunk {}", (void*)this);
  int32_t used_bytes = fragmented_size_.fetch_sub(1073741824, std::memory_order_acq_rel);
  pool_context_->stats.used_bytes -= used_bytes;

  uint32_t end = next_string_in_use_;
  next_string_in_use_ = 0;
  next_data_ = 0;

  spdlog::trace("Reallocating the strings");
  for (uint32_t i = 0; i != end; ++i) {
    InternalString* string = &strings_[strings_in_use_[i]];
    if (string->refcount_.load(std::memory_order_acq_rel) == 1) {
      spdlog::trace("Removing the string {}", (void*)string);
      pool_context_->set.erase(String((uint64_t)string));
      pool_context_->stats.num_strings -= 1;
    } else {
      std::swap(strings_in_use_[next_string_in_use_++], strings_in_use_[i]);
      reallocate(string);
    }
  }

  spdlog::trace("Reallocated the strings");
  pool_context_->stats.used_bytes += next_data_;
  fragmented_size_.fetch_add(1073741824 - used_bytes, std::memory_order_acq_rel);

  mutex_.Unlock();
}

void Chunk::reallocate(InternalString* s) {
  spdlog::trace("Reallocating the string {}", (void*)s);
  const char* from = s->data_;
  char* to = &data_[next_data_];
  s->data_ = to;
  next_data_ += s->size_;
  if (from != to) {
    // We compact the data in place \o/
    for (size_t i = s->size_; i; --i) {
      *to++ = *from++;
    }
  }
}


bool make_small_string(const std::string& str, uint64_t& result) {
  if (str.size() <= 7) {
    result = 0;
    for (ssize_t i = str.size()-1; i >= 0; --i) {
      result |= static_cast<uint8_t>(str[i]);
      result <<= 8;
    }
    result |= (str.size()<<2) | 1;

    return true;
  } else if (str.size() <= 10) {
    result = 0;
    for (ssize_t i = str.size()-1; i >= 0; --i) {
      result <<= 6;
      char c = ASCII_CHAR_TO_CODED_VALUE[static_cast<uint8_t>(str[i])];
      if (c == 127) return false;
      result |= c;
    }
    result <<= 4;
    result |= ((str.size()-8)<<2) | 3;

    return true;
  }

  return false;
}

String make_small_string(const std::string& str) {
  uint64_t data;
  assert(make_small_string(str, data));
  return String(data);
}

String make_string(const std::string& str, InternalString* internal_string) {
  uint64_t data;
  if (!make_small_string(str, data)) {
    //TODO remove this assert in the release build
    assert((((uint64_t)internal_string) & 1) == 0);
    internal_string->init(str, str.c_str(), nullptr);
    return String((uint64_t)internal_string);
  }

  return String(data);
}

} /* string */
} /* jmutils */
