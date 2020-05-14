#include "string_pool/pool.h"

namespace string_pool
{

string_t* alloc_string_ptr() {
  // We are going to use the less significative bit to store if the string
  // is a small string, for it we need the pointer to be even
  void* data = aligned_alloc(
    (sizeof(uintptr_t) & 1) ? (sizeof(uintptr_t)<<1) : sizeof(uintptr_t),
    sizeof(string_t)
  );
  if (data == nullptr) return nullptr;
  return new (data) string_t;
}

bool make_small_string(const std::string& str, uint64_t& result) {
  if (str.size() > 7) return false;

  result = 0;
  for (uint8_t c : str) {
    result |= c;
    result <<= 8;
  }
  result |= str.size()<<1;
  result |= 1;

  return true;
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

String::~String() {
  if (((data_ & 1) == 0) && (data_ != 0)) {
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
      // string_t pointer, since we can't destroy it until we remove the string
      // from the hashset
      if (fragmented_size > (CHUNK_DATA_SIZE>>1)) {
        ptr->chunk->pool->compact_chunk(ptr->chunk);
      }
    }
    data_ = 0;
  }
}

} /* string_pool */
