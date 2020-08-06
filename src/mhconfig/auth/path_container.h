#ifndef MHCONFIG__AUTH__PATH_CONTAINER_H
#define MHCONFIG__AUTH__PATH_CONTAINER_H

#include <string>
#include <string_view>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include "mhconfig/auth/common.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace auth
{

template <typename T>
class PathContainer
{
public:
  PathContainer() {
  }

  virtual ~PathContainer() {
  }

  bool init(
    std::vector<std::pair<std::string, T>>&& paths
  ) {
    return init(paths);
  }

  template <typename P>
  bool init(
    P&& paths
  ) {
    absl::flat_hash_map<std::string, split_t> splits;
    for (const auto& it : paths) {
      split_t split;

      split.parts = split_path_path(it.first);
      split.value = it.second;

      if (!are_valid_parts(split)) {
        return false;
      }

      std::string key;
      key.reserve(it.first.size()+1);
      for (auto part: split.parts) {
        key += '/';
        key += part;
      }

      splits[key] = std::move(split);
    }

    exact_path_.clear();
    prefix_path_ = prefix_node_t();

    std::vector<split_t> non_exact_splits;

    for (auto& it : splits) {
      if (it.second.is_exact) {
        exact_path_[it.first] = std::move(it.second.value);
      } else {
        if (it.second.has_wildcard) it.second.parts.pop_back();
        std::reverse(it.second.parts.begin(), it.second.parts.end());
        non_exact_splits.push_back(it.second);
      }
    }

    add_to_prefix_node(non_exact_splits, &prefix_path_);

    return true;
  }

  bool find(const std::string& path, T& value) const {
    auto parts = split_path_path(path);

    std::string key;
    key.reserve(path.size()+1);
    for (auto part: parts) {
      key += '/';
      key += part;
    }

    auto exact_search = exact_path_.find(key);
    if (exact_search != exact_path_.end()) {
      value = exact_search->second;
      return true;
    }

    return find_in_prefix_node(parts, &prefix_path_, value);
  }

private:
  struct prefix_node_t {
    bool terminal: 1;
    bool wildcard : 1;
    uint32_t refcount : 30;
    T value;
    T wildcard_value;
    absl::flat_hash_map<std::string, prefix_node_t*> exact;
    prefix_node_t* any;

    prefix_node_t()
      : terminal(false),
      wildcard(false),
      refcount(1),
      any(nullptr) {
    }

    ~prefix_node_t() {
      for (auto& it : exact) {
        if (--(it.second->refcount) == 0) {
          delete it.second;
        }
      }

      if (any != nullptr) {
        if (--(any->refcount) == 0) {
          delete any;
        }
      }
    }

    prefix_node_t* clone() const {
      auto result = new prefix_node_t;

      result->terminal = terminal;
      result->wildcard = wildcard;
      result->value = value;
      result->wildcard_value = wildcard_value;

      result->exact = exact;
      for (auto& it : exact) {
        it.second->refcount += 1;
      }

      if (any != nullptr) {
        any->refcount += 1;
        result->any = any;
      }

      return result;
    }
  };

  struct split_t {
    bool has_wildcard;
    bool is_exact;
    T value;
    std::vector<std::string_view> parts;
  };

  absl::flat_hash_map<std::string, T> exact_path_;
  prefix_node_t prefix_path_;

  std::vector<std::string_view> split_path_path(
    const std::string& path
  ) const {
    std::vector<std::string_view> result;
    size_t last_idx = 0;
    for (size_t i = 0, l = path.size(); i < l; ++i) {
      if (path[i] == '/') {
        if (i > last_idx) {
          result.emplace_back(&path[last_idx], i-last_idx);
        }
        last_idx = i+1;
      }
    }
    if (path.size() > last_idx) {
      result.emplace_back(&path[last_idx], path.size()-last_idx);
    }
    return result;
  }

  bool are_valid_parts(
    split_t& split
  ) {
    split.has_wildcard = false;
    split.is_exact = true;
    for (auto part : split.parts) {
      if (split.has_wildcard) {
        return false;
      }
      if (part == "*") {
        split.has_wildcard = true;
        split.is_exact = false;
      } else if (part == "+") {
        split.is_exact = false;
      } else {
        for (auto c : part) {
          if ((c == '*') || (c == '+')) return false;
        }
      }
    }
    return true;
  }

  void add_to_prefix_node(
    std::vector<split_t>& splits,
    prefix_node_t* node
  ) {
    absl::flat_hash_map<std::string, std::vector<split_t>> splits_by_key;
    for (size_t i = 0, l = splits.size(); i < l;) {
      if (splits[i].parts.empty()) {
        if (splits[i].has_wildcard) {
          node->wildcard = true;
          node->wildcard_value = splits[i].value;
        } else {
          node->terminal = true;
          node->value = splits[i].value;
        }

        jmutils::swap_delete(splits, i);
        --l;
      } else if (splits[i].parts.back() == "+") {
        splits[i].parts.pop_back();
        for (const auto& it : node->exact) {
          splits_by_key[it.first].push_back(splits[i]);
        }
        ++i;
      } else {
        auto key = splits[i].parts.back();
        splits[i].parts.pop_back();
        splits_by_key[key].push_back(splits[i]);
        jmutils::swap_delete(splits, i);
        --l;
      }
    }

    if (!splits.empty()) {
      if (node->any == nullptr) {
        node->any = new prefix_node_t;
      } else if (node->any->refcount > 1) {
        node->any->refcount -= 1;
        node->any = node->any->clone();
      }
      add_to_prefix_node(splits, node->any);
    }

    for (auto& it: splits_by_key) {
      prefix_node_t* new_node;
      auto search = node->exact.find(it.first);
      if (search != node->exact.end()) {
        if (search->second->refcount > 1) {
          search->second->refcount -= 1;
          search->second = search->second->clone();
        }
        new_node = search->second;
      } else {
        new_node = (node->any == nullptr)
          ? new prefix_node_t
          : node->any->clone();

        node->exact[it.first] = new_node;
      }
      add_to_prefix_node(it.second, new_node);
    }
  }

  inline bool find_in_prefix_node(
    const std::vector<std::string_view>& parts,
    const prefix_node_t* node,
    T& value
  ) const {
    const prefix_node_t* longest_prefix_node = nullptr;
    for (size_t i = 0, l = parts.size(); i < l; ++i) {
      if (node->wildcard) {
        longest_prefix_node = node;
      }
      auto search = node->exact.find(parts[i]);
      if (search != node->exact.end()) {
        node = search->second;
      } else {
        if (node->any == nullptr) {
          goto check_longest_prefix;
        }
        node = node->any;
      }
    }
    if (node->terminal) {
      value = node->value;
      return true;
    }

    if (node->wildcard) {
      value = node->wildcard_value;
      return true;
    }

check_longest_prefix:
    if (longest_prefix_node != nullptr) {
      value = longest_prefix_node->wildcard_value;
      return true;
    }

    return false;
  }

};

} /* auth */
} /* mhconfig */

#endif
