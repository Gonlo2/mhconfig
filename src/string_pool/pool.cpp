#include "string_pool/pool.h"

namespace string_pool
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
    std::shared_lock lock(chunk_->mutex_);
    return std::string(data_, size_);
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
        if (chunk_->decrement_refcount()) {
          chunk_->~Chunk();
          free(chunk_);
        }
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

String::String(String&& o) noexcept : data_(o.data_) {
  o.data_ = 0;
}

String& String::operator=(const String& o) noexcept {
  data_ = o.data_;
  if (!is_small() && (data_ != 0)) {
    ((InternalString*) data_)->increment_refcount();
  }
  return *this;
}

String& String::operator=(String&& o) noexcept {
  data_ = o.data_;
  o.data_ = 0;
  return *this;
}

String::~String() {
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
        case 1:
          result += CODED_VALUE_TO_ASCII_CHAR[data & 63];
          data >>= 6;
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
      }
    } else {
      switch ((data_>>2) & 7) {
        case 7:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 6:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 5:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 4:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 3:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 2:
          data >>= 8;
          result += static_cast<char>(data & 255);
        case 1:
          data >>= 8;
          result += static_cast<char>(data & 255);
      }
    }
    return result;
  }

  return ((InternalString*) data_)->str();
}


Pool::Pool()
  : stats_observer_(nullptr)
{
}

Pool::Pool(
  std::unique_ptr<StatsObserver>&& stats_observer
)
  : stats_observer_(std::move(stats_observer))
{
}

Pool::~Pool() {
  for (Chunk* chunk : chunks_) {
    if (chunk->remove_pool()) {
      chunk->~Chunk();
      free(chunk);
    }
  }

  stats_.num_strings = 0;
  stats_.num_chunks = 0;
  stats_.reclaimed_bytes = 0;
  stats_.used_bytes = 0;

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(stats_, true);
  }
}

const String Pool::add(const std::string& str) {
  InternalString internal_string;
  String s = make_string(str, &internal_string);
  if (!s.is_small()) {
    //TODO Rethink the string build part to avoid create the str and
    //change the unique mutex with a shared one to allow parallel
    //calls if the string already exists in the set
    std::unique_lock lock(mutex_);

    auto search = set_.find(s);
    if (search != set_.end()) {
      return *search;
    }

    spdlog::trace("Adding a new string");
    return *set_.insert(store_string(str)).first;
  }

  return s;
}

const stats_t& Pool::stats() const {
  return stats_;
}

void Pool::compact() {
  std::unique_lock lock_chunk(mutex_);
  spdlog::trace("Compacting the chunks");

  for (Chunk* chunk : chunks_) {
    chunk->compact(set_, stats_);
  }
}

const String Pool::store_string(const std::string& str) {
  spdlog::trace("Adding a new string of size {}", str.size());
  stats_.num_strings += 1;

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

  auto result = chunks_[idx]->add(str, stats_);

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(stats_, false);
  }

  return result;
}

Chunk* Pool::new_chunk() {
  spdlog::trace("Making a new chunk");
  stats_.num_chunks += 1;
  stats_.reclaimed_bytes += CHUNK_DATA_SIZE;

  void* data = aligned_alloc(8, sizeof(Chunk));
  assert (data != nullptr);
  return new (data) Chunk(this);
}


Chunk::Chunk(Pool* pool) {
  refcount_.store(1);
  fragmented_size_.store(0);
  next_data_ = 0;
  next_string_in_use_ = 0;
  pool_ = pool;

  for (size_t i = 0; i < CHUNK_STRING_SIZE; ++i) {
    strings_in_use_[i] = i;
  }
}

Chunk::~Chunk() {
}


bool Chunk::remove_pool() {
  std::unique_lock lock_chunk(mutex_);
  pool_ = nullptr;
  return decrement_refcount();
}

void Chunk::released_string(uint32_t size) {
  ssize_t fragmented_size = align(size);
  fragmented_size += fragmented_size_.fetch_add(
    fragmented_size,
    std::memory_order_relaxed
  );

  spdlog::trace(
    "Checking if it's possible compact the chunk (fragmented_size: {}, limit: {})",
    fragmented_size,
    CHUNK_DATA_SIZE>>1
  );
  if (fragmented_size > (CHUNK_DATA_SIZE>>1)) {
    std::unique_lock lock_chunk(mutex_);
    if ((pool_ != nullptr) && (fragmented_size_.load(std::memory_order_acq_rel) > (CHUNK_DATA_SIZE>>1))) {
      std::unique_lock lock_pool(pool_->mutex_);
      unsafe_compact(pool_->set_, pool_->stats_);
    }
  }
}

String Chunk::add(const std::string& str, stats_t& stats) {
  std::unique_lock lock_chunk(mutex_);

  refcount_.fetch_add(1, std::memory_order_relaxed);

  spdlog::trace("Adding a string of size {} in {}", str.size(), (void*)this);
  strings_[next_string_in_use_].init(str, &data_[next_data_], this);
  memcpy(&data_[next_data_], str.c_str(), str.size());

  size_t data_size = align(str.size());
  spdlog::trace("Moving the next_data pointer {} bytes", data_size);
  next_data_ += data_size;
  stats.used_bytes += data_size;

  return String((uint64_t)&strings_[next_string_in_use_++]);
}

void Chunk::unsafe_compact(std::unordered_set<String>& set, stats_t& stats) {
  spdlog::trace("Compacting the chunk {}", (void*)this);
  int32_t used_bytes = fragmented_size_.fetch_sub(1073741824, std::memory_order_acq_rel);
  stats.used_bytes -= used_bytes;

  uint32_t end = next_string_in_use_;
  next_string_in_use_ = 0;
  next_data_ = 0;

  spdlog::trace("Reallocating the strings");
  for (uint32_t i = 0; i != end; ++i) {
    InternalString* string = &strings_[strings_in_use_[i]];
    if (string->refcount_.load(std::memory_order_acq_rel) == 1) {
      spdlog::trace("Removing the string {}", (void*)string);
      set.erase(String((uint64_t)string));
      stats.num_strings -= 1;
    } else {
      std::swap(strings_in_use_[next_string_in_use_++], strings_in_use_[i]);
      reallocate(string);
    }
  }

  spdlog::trace("Reallocated the strings");
  stats.used_bytes += next_data_;
  fragmented_size_.fetch_add(1073741824 - used_bytes, std::memory_order_acq_rel);
}

void Chunk::reallocate(InternalString* s) {
  spdlog::trace("Reallocating the string {}", (void*)s);
  const char* from = s->data_;
  char* to = &data_[next_data_];
  s->data_ = to;
  next_data_ += align(s->size_);
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

} /* string_pool */
