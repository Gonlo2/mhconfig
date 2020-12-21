#ifndef JMUTILS__STRUCTURES__LABEL_SET_H
#define JMUTILS__STRUCTURES__LABEL_SET_H

#include <assert.h>
#include <boost/container_hash/extensions.hpp>
#include <boost/functional/hash.hpp>
#include <ext/alloc_traits.h>
#include <stddef.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

#include "jmutils/container/weak_container.h"

namespace jmutils
{
namespace container
{
template <typename T> class WeakContainer;

using label_t = std::pair<std::string, std::string>;

class Labels final
{
public:
  using iterator = std::vector<label_t>::const_iterator;
  using const_iterator = std::vector<label_t>::const_iterator;

  explicit Labels() {
  }

  size_t size() const {
    return labels_.size();
  }

  bool empty() const {
    return labels_.empty();
  }

  iterator begin() const {
    return labels_.cbegin();
  }

  iterator end() const {
    return labels_.cend();
  }

  const_iterator cbegin() const {
    return labels_.cbegin();
  }

  const_iterator cend() const {
    return labels_.cend();
  }

  bool contains(const Labels& subset) const {
    auto it = cbegin();
    auto end = cend();
    for (const auto& label : subset) {
      for (;(it != end) && (*it < label); ++it);
      if ((it == end) || (*it != label)) return false;
      ++it;
    }
    return true;
  }

  template <typename H>
  friend H AbslHashValue(H h, const Labels& labels) {
    return H::combine(std::move(h), labels.hash_);
  }

  std::string repr() const;

private:
  size_t hash_{0};
  std::vector<label_t> labels_;

  template <typename V>
  explicit Labels(V&& labels)
    : labels_(std::forward<V>(labels))
  {
    for (const auto& it : labels_) {
      boost::hash_combine(hash_, it.first);
      boost::hash_combine(hash_, it.second);
    }
  }

  friend Labels make_labels(std::vector<label_t>&& labels);
  friend bool operator==(const Labels& lhs, const Labels& rhs);
};

Labels make_labels(
  std::vector<label_t>&& labels
);

const static Labels EMPTY_LABELS;

inline bool operator==(const Labels& lhs, const Labels& rhs) {
  if (&lhs == &rhs) return true;
  if (lhs.hash_ != rhs.hash_) return false;
  size_t l = lhs.size();
  if (l != rhs.labels_.size()) return false;

  for (size_t i = 0; i < l; ++i) {
    if (lhs.labels_[i] != rhs.labels_[i]) return false;
  }

  return true;
}

template <typename T>
class LabelSet
{
private:
  struct entry_t {
    Labels labels;
    T value;
  };

  using entry_ptr_t = std::shared_ptr<entry_t>;

  absl::flat_hash_map<label_t, std::unique_ptr<jmutils::container::WeakContainer<entry_t>>> entries_by_label_;
  absl::flat_hash_map<Labels, entry_ptr_t> entry_by_labels_;

public:
  T* get(
    const Labels& labels
  ) const {
    auto search = entry_by_labels_.find(labels);
    return search == entry_by_labels_.end() ? nullptr : &search->second->value;
  }

  T* get_or_build(
    const Labels& labels
  ) {
    auto entry = get_or_build_entry(labels);
    return &entry->value;
  }

  template <typename V>
  void insert(
    const Labels& labels,
    V&& value
  ) {
    auto entry = get_or_build_entry(labels);
    entry->value = std::move(value);
  }

  template <typename L>
  bool for_each(
    L lambda
  ) {
    for (auto& it : entry_by_labels_) {
      bool ok = lambda(
        static_cast<const Labels&>(it.second->labels),
        static_cast<T*>(&(it.second->value))
      );
      if (!ok) return false;
    }
    return true;
  }

  template <typename L>
  bool for_each_subset(
    const Labels& labels,
    L lambda
  ) {
    absl::flat_hash_map<entry_ptr_t, size_t> count_by_entry_ptr;

    if (auto s = entry_by_labels_.find(Labels()); s != entry_by_labels_.end()) {
      count_by_entry_ptr[s->second] = 0;
    }

    for (const auto& label : labels) {
      auto search = entries_by_label_.find(label);
      if (search != entries_by_label_.end()) {
        search->second->for_each(
          [&count_by_entry_ptr](auto&& ptr) {
            count_by_entry_ptr[ptr] += 1;
          }
        );
      }
    }

    for (auto& it : count_by_entry_ptr) {
      if (it.first->labels.size() == it.second) {
        bool ok = lambda(
          static_cast<const Labels&>(it.first->labels),
          static_cast<T*>(&(it.first->value))
        );
        if (!ok) return false;
      }
    }

    return true;
  }

  void remove(
    const Labels& labels
  ) {
    entry_by_labels_.erase(labels);
  }

  bool empty() const {
    return entry_by_labels_.empty();
  }

private:
  entry_t* get_or_build_entry(
    const Labels& labels
  ) {
    if (auto search = entry_by_labels_.find(labels); search != entry_by_labels_.end()) {
      return search->second.get();
    }

    auto entry = std::make_shared<entry_t>();
    entry->labels = labels;

    for (const auto& label : labels) {
      auto search = entries_by_label_.find(label);
      if (search != entries_by_label_.end()) {
        search->second->add(entry);
      } else {
        auto entries = std::make_unique<jmutils::container::WeakContainer<entry_t>>();
        entries->add(entry);
        auto r = entries_by_label_.emplace(label, std::move(entries));
        assert(r.second);
      }
    }

    auto r = entry_by_labels_.emplace(labels, std::move(entry));
    assert(r.second);

    return r.first->second.get();
  }

};

} /* container */
} /* jmutils */

#endif
