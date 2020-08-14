#ifndef JMUTILS__STRUCTURES__WEAK_MULTIMAP_H
#define JMUTILS__STRUCTURES__WEAK_MULTIMAP_H

#include <vector>
#include <absl/synchronization/mutex.h>
#include <absl/container/flat_hash_map.h>

#include "jmutils/container/weak_container.h"

namespace jmutils
{
namespace container
{

template <typename K, typename T>
class WeakMultimap final
{
public:
  template <typename V>
  void insert(const K& key, V&& value) {
    bool added = false;

    mutex_.ReaderLock();
    auto search = values_.find(key);
    if (search != values_.end()) {
      search->second.add(std::forward<V>(value));
      added = true;
    }
    mutex_.ReaderUnlock();

    if (!added) {
      mutex_.Lock();
      values_[key].add(std::forward<V>(value));
      mutex_.Unlock();
    }
  }

  template <typename L>
  size_t for_each(const K& key, L lambda) {
    bool has_values = false;
    size_t container_size = 0;

    mutex_.ReaderLock();
    auto search = values_.find(key);
    if (search != values_.end()) {
      has_values = true;
      container_size = search->second.for_each(lambda);
    }
    mutex_.ReaderUnlock();

    if (has_values && (container_size == 0)) {
      mutex_.Lock();
      auto search = values_.find(key);
      if (search != values_.end()) {
        container_size = search->second.remove_expired();
        if (container_size == 0) values_.erase(search);
      }
      mutex_.Unlock();
    }

    return container_size;
  }

  void remove_expired() {
    std::vector<K> to_remove_;

    mutex_.ReaderLock();
    for (auto& it : values_) {
      if (it.second.remove_expired() == 0) {
        to_remove_.push_back(it.first);
      }
    }
    mutex_.ReaderUnlock();

    if (!to_remove_.empty()) {
      mutex_.Lock();
      for (size_t i = 0, l = to_remove_.size(); i < l; ++i) {
        auto search = values_.find(to_remove_[i]);
        if (search != values_.end()) {
          if (search->second.remove_expired() == 0) {
            values_.erase(search);
          }
        }
      }
      mutex_.Unlock();
    }
  }

  inline bool empty() {
    mutex_.ReaderLock();
    bool empty = values_.empty();
    mutex_.ReaderUnlock();
    return empty;
  }

private:
  absl::Mutex mutex_;
  absl::flat_hash_map<K, WeakContainer<T>> values_;
};

} /* container */
} /* jmutils */

#endif
