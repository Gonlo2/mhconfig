#ifndef JMUTILS__STRUCTURES__WEAK_CONTAINER_H
#define JMUTILS__STRUCTURES__WEAK_CONTAINER_H

#include <vector>
#include <absl/synchronization/mutex.h>
#include "jmutils/common.h"

namespace jmutils
{
namespace container
{

template <typename T>
class WeakContainer
{
public:
  WeakContainer() {
  }

  WeakContainer(WeakContainer&& o) {
    o.mutex_.Lock();
    std::swap(values_, o.values_);
    o.mutex_.Unlock();
  }

  template <typename V>
  void add(V&& value) {
    mutex_.Lock();
    values_.push_back(std::forward<V>(value));
    mutex_.Unlock();
  }

  template <typename L>
  size_t for_each(L lambda) {
    std::vector<size_t> to_remove_;

    mutex_.ReaderLock();
    for (size_t i = 0, l = values_.size(); i < l; ++i) {
      if (auto value = values_[i].lock()) {
        lambda(std::move(value));
      } else {
        to_remove_.push_back(i);
      }
    }
    size_t container_size = values_.size() - to_remove_.size();
    mutex_.ReaderUnlock();

    return to_remove_.empty()
      ? container_size
      : try_remove(to_remove_);
  }

  template <typename L>
  void consume(L lambda) {
    std::vector<std::weak_ptr<T>> values;

    mutex_.Lock();
    std::swap(values, values_);
    mutex_.Unlock();

    for (size_t i = 0, l = values.size(); i < l; ++i) {
      if (auto value = values[i].lock()) {
        lambda(std::move(value));
      }
    }
  }

  size_t remove_expired() {
    std::vector<size_t> to_remove_;

    mutex_.ReaderLock();
    for (size_t i = 0, l = values_.size(); i < l; ++i) {
      if (values_[i].expired()) {
        to_remove_.push_back(i);
      }
    }
    size_t container_size = values_.size() - to_remove_.size();
    mutex_.ReaderUnlock();

    return to_remove_.empty()
      ? container_size
      : try_remove(to_remove_);
  }

  bool empty() {
    mutex_.ReaderLock();
    bool empty = values_.empty();
    mutex_.ReaderUnlock();
    return empty;
  }

private:
  mutable absl::Mutex mutex_;
  std::vector<std::weak_ptr<T>> values_;

  size_t try_remove(std::vector<size_t>& positions) {
    mutex_.Lock();
    while (!positions.empty() && (values_.size() <= positions.back())) {
      positions.pop_back();
    }
    size_t container_size = values_.size();
    for (size_t i = 0, l = positions.size(); i < l; ++i) {
      if (size_t idx = positions[i]; values_[idx].expired()) {
        jmutils::swap_delete(values_, idx);
        --container_size;
      }
    }
    mutex_.Unlock();

    return container_size;
  }

};

} /* container */
} /* jmutils */

#endif
