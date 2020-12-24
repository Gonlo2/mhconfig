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
#include <absl/container/flat_hash_set.h>
#include <absl/hash/hash.h>

#include "jmutils/container/weak_container.h"
#include "spdlog/spdlog.h"

#include <fmt/format.h>

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

  const label_t& back() const {
    return labels_.back();
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
public:
  LabelSet() {
    root_ = std::make_unique<node_t>();
  }

  T* get(
    const Labels& labels
  ) const {
    auto search = node_by_labels_.find(labels);
    return search == node_by_labels_.end()
      ? nullptr
      : &search->second->entry->value;
  }

  T* get_or_build(
    const Labels& labels
  ) {
    auto node = get_or_build_node(labels);
    return &node->entry->value;
  }

  template <typename V>
  void insert(
    const Labels& labels,
    V&& value
  ) {
    auto entry = get_or_build_node(labels);
    entry->entry->value = std::move(value);
  }

  template <typename L>
  bool for_each(
    L lambda
  ) {
    for (auto& it : node_by_labels_) {
      bool ok = lambda(
        static_cast<const Labels&>(it.second->entry->labels),
        static_cast<T*>(&(it.second->entry->value))
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
    std::vector<absl::flat_hash_map<node_t*, ll_head_t>> nodes_by_depth;
    absl::flat_hash_set<label_t> labels_set;

    nodes_by_depth.resize(1);

    for (const auto& label : labels) {
      labels_set.insert(label);
      auto search = nodes_by_last_label_.find(label);
      if (search != nodes_by_last_label_.end()) {
        for (node_t* node: search->second) {
          if (node->depth > nodes_by_depth.size()) {
            nodes_by_depth.resize(node->depth);
          }

          spdlog::trace(
            "The entry with labels {} has depth {}",
            node->entry->labels,
            node->depth
          );

          auto& ll_head = nodes_by_depth[node->depth-1][node];
          ll_head.first = std::make_unique<ll_node_t>();
          ll_head.first->entry = node->entry.get();
          ll_head.last = ll_head.first.get();
        }
      }
    }

    for (ssize_t i = nodes_by_depth.size()-1; i >= 1; --i) {
      for (auto& it : nodes_by_depth[i]) {
        node_t* parent = it.first->parent;
        spdlog::trace(
          "Checking label '{}/{}'",
          parent->label.first,
          parent->label.second
        );
        if (labels_set.contains(parent->label)) {
          spdlog::trace(
            "The label '{}/{}' exists in the depth {}",
            parent->label.first,
            parent->label.second,
            i
          );
          auto& ll_head = nodes_by_depth[i-1][parent];
          if (ll_head.first == nullptr) {
            ll_head = std::move(it.second);
          } else {
            ll_head.last->next = std::move(it.second.first);
            ll_head.last = it.second.last;
          }
        }
      }
    }

    if (root_->entry != nullptr) {
      bool ok = lambda(
        static_cast<const Labels&>(root_->entry->labels),
        static_cast<T*>(&(root_->entry->value))
      );
      if (!ok) return false;
    }

    for (auto& it : nodes_by_depth[0]) {
      auto ll_node = it.second.first.get();
      while (ll_node != nullptr) {
        bool ok = lambda(
          static_cast<const Labels&>(ll_node->entry->labels),
          static_cast<T*>(&(ll_node->entry->value))
        );
        if (!ok) return false;
        ll_node = ll_node->next.get();
      }
    }

    return true;
  }

  bool remove(
    const Labels& labels
  ) {
    node_t* node;
    if (auto search = node_by_labels_.extract(labels)) {
      node = search.mapped();
    } else {
      return false;
    }

    node->entry = nullptr;

    if (!labels.empty()) {
      if (
        auto search = nodes_by_last_label_.find(labels.back());
        search != nodes_by_last_label_.end()
      ) {
        for (size_t i = 0, l = search->second.size(); i < l; ++i) {
          if (search->second[i] == node) {
            jmutils::swap_delete(search->second, i);
            break;
          }
        }
        if (search->second.empty()) {
          nodes_by_last_label_.erase(search);
        }
      } else {
        assert(false);
      }
      while (node->parent != nullptr) {
        if (node->first_child != nullptr) break;
        auto child = extract_child(node->parent, node);
        assert(child != nullptr);
        node = child->parent;
      }
    }

    return true;
  }

  bool empty() const {
    return node_by_labels_.empty();
  }

private:
  struct entry_t {
    Labels labels;
    T value;
  };

  struct node_t {
    label_t label;
    uint16_t depth{0};
    node_t* parent{nullptr};
    std::unique_ptr<node_t> first_child{nullptr};
    std::unique_ptr<node_t> next_sibling{nullptr};
    std::unique_ptr<entry_t> entry{nullptr};
  };

  struct ll_node_t {
    entry_t* entry{nullptr};
    std::unique_ptr<ll_node_t> next{nullptr};
  };

  struct ll_head_t {
    std::unique_ptr<ll_node_t> first{nullptr};
    ll_node_t* last{nullptr};
  };

  std::unique_ptr<node_t> root_;
  absl::flat_hash_map<label_t, std::vector<node_t*>> nodes_by_last_label_;
  absl::flat_hash_map<Labels, node_t*> node_by_labels_;

  node_t* get_or_build_node(
    const Labels& labels
  ) {
    if (
      auto search = node_by_labels_.find(labels);
      search != node_by_labels_.end()
    ) {
      return search->second;
    }

    auto node = root_.get();
    for (const auto& label : labels) {
      if (auto search = get_sibling(label, node->first_child.get())) {
        node = search;
      } else {
        auto child = std::make_unique<node_t>();
        child->label = label;
        child->depth = node->depth+1;
        child->parent = node;
        child->next_sibling = std::move(node->first_child);
        node->first_child = std::move(child);
        node = node->first_child.get();
      }
    }

    node->entry = std::make_unique<entry_t>();
    node->entry->labels = labels;

    node_by_labels_[labels] = node;

    if (!labels.empty()) {
      nodes_by_last_label_[labels.back()].push_back(node);
    }

    return node;
  }

  node_t* get_sibling(
    const label_t& label,
    node_t* node
  ) {
    while ((node != nullptr) && (node->label != label)) {
      node = node->next_sibling.get();
    }
    return node;
  }

  std::unique_ptr<node_t> extract_child(
    node_t* parent,
    node_t* child
  ) {
    if (parent->first_child == nullptr) {
      return nullptr;
    }

    if (parent->first_child.get() == child) {
      auto result = std::move(parent->first_child);
      parent->first_child = std::move(result->next_sibling);
      return result;
    }

    auto node = parent->first_child.get();
    while ((node->next_sibling != nullptr) && (node->next_sibling.get() != child)) {
      node = node->next_sibling.get();
    }
    if (node->next_sibling.get() != child) return nullptr;

    auto result = std::move(node->next_sibling);
    node->next_sibling = std::move(result->next_sibling);
    return result;
  }

};

} /* container */
} /* jmutils */


template <> struct fmt::formatter<jmutils::container::Labels>: formatter<string_view> {
  template <typename FormatContext>
  auto format(const jmutils::container::Labels& labels, FormatContext& ctx) {
    auto oit = format_to(ctx.out(), "(");
    auto it = labels.cbegin();
    if (it != labels.cend()) {
      oit = format_to(oit, "'{}': '{}'", it->first, it->second);
      ++it;
    }
    while (it != labels.cend()) {
      oit = format_to(oit, ", '{}': '{}'", it->first, it->second);
      ++it;
    }
    return format_to(oit, ")");
  }
};


#endif
