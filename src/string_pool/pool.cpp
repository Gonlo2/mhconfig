#include "string_pool/pool.h"

namespace string_pool
{

string_t* alloc_string_ptr() {
  // We are going to use the less significative bit to store if the string
  // is a small string, for it we need the pointer to be even
  void* data = aligned_alloc(8, sizeof(string_t));
  if (data == nullptr) return nullptr;
  return new (data) string_t;
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

string_t* make_string_ptr(const std::string& str, chunk_t* chunk) {
  string_t* result = alloc_string_ptr();
  assert(result != nullptr);

  init_string(str, chunk->next_data, result);
  memcpy(chunk->next_data, str.c_str(), str.size());
  result->chunk = chunk;
  if (chunk->last_string == nullptr) {
    chunk->first_string = result;
  } else {
    chunk->last_string->next = result;
  }
  chunk->last_string = result;

  return result;
}


String::String() noexcept {
  data_ = 0;
}

String::String(const std::string& str) noexcept {
  if (!make_small_string(str, data_)) {
    void* data = alloc_string_ptr();
    assert (data != nullptr);
    string_t* ptr = new (data) string_t;
    init_string(str, (char*) str.c_str(), ptr);
    ptr->refcount.fetch_add(1, std::memory_order_relaxed);
    data_ = (uint64_t)ptr;
  }
}

String::String(uint64_t data) noexcept : data_(data) {
  if (!is_small() && (data_ != 0)) {
    string_t* ptr = (string_t*) data_;
    ptr->refcount.fetch_add(1, std::memory_order_relaxed);
  }
}

String::String(const String& o) noexcept : data_(o.data_) {
  if (!is_small() && (data_ != 0)) {
    string_t* ptr = (string_t*) data_;
    ptr->refcount.fetch_add(1, std::memory_order_relaxed);
  }
}

String::String(String&& o) noexcept : data_(o.data_) {
  o.data_ = 0;
}

String& String::operator=(const String& o) noexcept {
  data_ = o.data_;
  if (!is_small() && (data_ != 0)) {
    string_t* ptr = (string_t*) data_;
    ptr->refcount.fetch_add(1, std::memory_order_relaxed);
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
    string_t* ptr = (string_t*) data_;
    uint64_t refcount = ptr->refcount.fetch_sub(1, std::memory_order_acq_rel);
    // This logic is tricky, if this is the last reference we could drop the string
    if (refcount == 1) {
      ptr->~string_t();
      free(ptr);
    // In other case we need to check if some chunk is asigned and exists another
    // object that point to the string (in this case this object is the hashset
    // to check if the object is present)
    } else if ((ptr->chunk != nullptr) && (refcount == 2)) {
      // If the last reference is the hashset we check if we could reclaim the half
      // of the chunk data compacting it
      size_t fragmented_size = align(ptr->size);
      fragmented_size += ptr->chunk->fragmented_size.fetch_add(
        fragmented_size,
        std::memory_order_relaxed
      );

      // Take in mind that the compacter process will reclaim also the space of the
      // string_t pointer, since we can't destroy it until we have removed
      // the string from the hashset
      if (fragmented_size > (CHUNK_DATA_SIZE>>1)) {
        std::unique_lock lock_pool(ptr->chunk->pool->mutex_);
        ptr->chunk->pool->compact_chunk(ptr->chunk);
      }
    }
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

  string_t* ptr = (string_t*) data_;
  if (ptr->chunk != nullptr) {
    std::shared_lock lock(ptr->chunk->mutex);
    return std::string(ptr->data, ptr->size);
  }

  return std::string(ptr->data, ptr->size);
}


Pool::Pool()
  : stats_observer_(nullptr)
{
  head_ = new_chunk();
}

Pool::Pool(
  std::unique_ptr<StatsObserver>&& stats_observer
)
  : stats_observer_(std::move(stats_observer))
{
  head_ = new_chunk();
}

Pool::~Pool() {
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

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(stats_, true);
  }
}

const String Pool::add(const std::string& str) {
  String s(str);
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

  for (chunk_t* chunk = head_; chunk != nullptr; chunk = chunk->next) {
    compact_chunk(chunk);
  }
}

const String Pool::store_string(const std::string& str) {
  spdlog::trace("Adding a new string of size {}", str.size());
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

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(stats_, false);
  }

  return String((uint64_t)string);
}

void Pool::compact_chunk(chunk_t* chunk) {
  std::unique_lock lock_chunk(chunk->mutex);

  spdlog::trace("Compacting the chunk {}", (void*)chunk);

  stats_.used_bytes -= chunk->fragmented_size.exchange(0, std::memory_order_acq_rel);

  spdlog::trace("Looking for the first string");
  string_t* next_string = chunk->first_string;
  string_t* last_string = nullptr;
  chunk->next_data = chunk->data;
  chunk->first_string = nullptr;
  while (next_string != nullptr) {
    string_t* current_string = next_string;
    next_string = next_string->next;
    if (current_string->refcount.load(std::memory_order_acq_rel) == 1) {
      remove_string(current_string);
    } else {
      chunk->first_string = current_string;
      last_string = current_string;
      reallocate_string(chunk, current_string);
      break;
    }
  }

  spdlog::trace("Reallocating the strings");
  while (next_string != nullptr) {
    string_t* current_string = next_string;
    next_string = next_string->next;
    if (current_string->refcount.load(std::memory_order_acq_rel) == 1) {
      remove_string(current_string);
    } else {
      last_string->next = current_string;
      last_string = current_string;
      reallocate_string(chunk, current_string);
    }
  }

  if (last_string != nullptr) last_string->next = nullptr;
  chunk->last_string = last_string;

  stats_.used_bytes += chunk->next_data - chunk->data;

  if (stats_observer_ != nullptr) {
    stats_observer_->on_updated_stats(stats_, true);
  }
}

void Pool::remove_string(string_t* s) {
  spdlog::trace("Removing the string {}", (void*)s);
  // This is the hacky/tricky logic that avoid call recursively to compact
  // the same chunk after erase the new string, since we defined as null
  // the chunk the destructor will avoid the compacter branch and remove
  // successfully the string at the end :magic:
  s->chunk = nullptr;
  set_.erase(String((uint64_t)s));
  stats_.num_strings -= 1;
}

void Pool::reallocate_string(chunk_t* chunk, string_t* s) {
  spdlog::trace("Reallocating the string {}", (void*)s);
  char* from = s->data;
  char* to = chunk->next_data;
  s->data = to;
  size_t data_size = align(s->size);
  chunk->next_data += data_size;
  if (from != to) {
    // We compact the data in place \o/
    while (to != chunk->next_data) *to++ = *from++;
  }
}

chunk_t* Pool::new_chunk() {
  spdlog::trace("Making a new chunk");
  stats_.num_chunks += 1;
  stats_.reclaimed_bytes += CHUNK_DATA_SIZE;

  void* data = aligned_alloc(8, sizeof(chunk_t));
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

} /* string_pool */
