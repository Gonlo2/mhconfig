#include "string_pool/pool.h"

namespace string_pool
{

string_t* make_string_ptr(const std::string& str, chunk_t* chunk) {
  void* data = aligned_alloc(sizeof(size_t), sizeof(string_t));
  string_t* result = new (data) string_t;
  assert(result != nullptr);

  init_string(str, chunk->next_data, result, true);
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
  if ((ptr_ != nullptr) && ptr_->needs_to_be_destroyed) {
    uint64_t refcount = ptr_->refcount.fetch_sub(1, std::memory_order_relaxed);
    // This logic is tricky, if this is the last reference we could drop the string
    if (refcount == 1) {
      ptr_->~string_t();
      free(ptr_);
    // In other case we need to check if some chunk is asigned and exists another
    // object that point to the string (in this case this object is the hashset
    // to check if the object is present)
    } else if ((ptr_->chunk != nullptr) && (refcount == 2)) {
      // If the last reference is the hashset we check if we could reclaim the half
      // of the chunk data compacting it
      size_t fragmented_size = align(ptr_->size);
      fragmented_size += ptr_->chunk->fragmented_size.fetch_add(
        fragmented_size,
        std::memory_order_relaxed
      );

      // Take in mind that the compacter process will reclaim also the space of the
      // string_t pointer, since we can't destroy it until we remove the string
      // from the hashset
      if (fragmented_size > (CHUNK_DATA_SIZE>>1)) {
        ptr_->chunk->pool->compact_chunk(ptr_->chunk);
      }
    }
    ptr_ = nullptr;
  }
}

} /* string_pool */
