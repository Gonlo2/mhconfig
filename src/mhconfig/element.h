#ifndef MHCONFIG__ELEMENT_H
#define MHCONFIG__ELEMENT_H

#include <vector>
#include <utility>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <variant>

#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"
#include "jmutils/string/pool.h"
#include "jmutils/box.h"
#include <fmt/format.h>
#include <boost/algorithm/string.hpp>

#include <absl/container/flat_hash_map.h>

namespace mhconfig {
  enum class NodeType {
    UNDEFINED_NODE = 0,
    MAP_NODE = 1,
    SEQUENCE_NODE = 2,
    NULL_NODE = 3,
    STR_NODE = 4,
    INT_NODE = 5,
    FLOAT_NODE = 6,
    BOOL_NODE = 7,

    // Virtual nodes
    FORMAT_NODE = 8,  // Sequence
    SREF_NODE = 9,  // Sequence
    REF_NODE = 10,  // Sequence
    DELETE_NODE = 11, // None
    OVERRIDE_MAP_NODE = 12, // Map
    OVERRIDE_SEQUENCE_NODE = 13, // Sequence
    OVERRIDE_NULL_NODE = 14, // None
    OVERRIDE_STR_NODE = 15, // Str
    OVERRIDE_INT_NODE = 16, // Int
    OVERRIDE_FLOAT_NODE = 17, // Float
    OVERRIDE_BOOL_NODE = 18 // Bool
  };

  class Element;

  typedef jmutils::string::String Literal;
  typedef absl::flat_hash_map<jmutils::string::String, Element> Map;
  typedef std::vector<Element> Sequence;
  typedef jmutils::Box<Map, uint32_t> MapBox;
  typedef jmutils::Box<Sequence, uint32_t> SequenceBox;

  struct dummy_t {};

  typedef std::variant<
    dummy_t, // UNDEFINED_NODE = 0,
    MapBox*, // MAP_NODE = 1,
    SequenceBox*, // SEQUENCE_NODE = 2,
    dummy_t, // NULL_NODE = 3,
    Literal, // STR_NODE = 4,
    int64_t, // INT_NODE = 5,
    double, // FLOAT_NODE = 6,
    bool, // BOOL_NODE = 7,

    // Virtual nodes
    SequenceBox*, // FORMAT_NODE = 8,  // Sequence
    SequenceBox*, // SREF_NODE = 9,  // Sequence
    SequenceBox*, // REF_NODE = 10,  // Sequence
    dummy_t, // DELETE_NODE = 11, // Whatever
    MapBox*, // OVERRIDE_MAP_NODE = 12, // Map
    SequenceBox*, // OVERRIDE_SEQUENCE_NODE = 13 // Sequence
    dummy_t, // OVERRIDE_NULL_NODE = 14,
    Literal, // OVERRIDE_STR_NODE = 15,
    int64_t, // OVERRIDE_INT_NODE = 16,
    double, // OVERRIDE_FLOAT_NODE = 17,
    bool // OVERRIDE_BOOL_NODE = 18,
  > Data;

  namespace {
    template <NodeType I, typename T>
    constexpr auto& node_get(T&& v) {
      return std::get<static_cast<size_t>(I)>(v);
    }

    template <NodeType I, class T, typename... Args>
    constexpr auto& node_set(T&& v, Args&&... args) {
      return v.template emplace<static_cast<size_t>(I)>(std::forward<Args>(args)...);
    }
  }

  namespace conversion
  {
    template <typename T>
    std::pair<bool, T> as(const Data& data);
  } /* conversion */

  std::string to_string(NodeType type);

  NodeType scalar_type(const Literal& tag);

  class Element final
  {
  public:
    Element() noexcept;
    explicit Element(NodeType type) noexcept;
    explicit Element(const Literal& value, bool override_ = false) noexcept;
    explicit Element(const int64_t value, bool override_ = false) noexcept;
    explicit Element(const double value, bool override_ = false) noexcept;
    explicit Element(const bool value, bool override_ = false) noexcept;
    explicit Element(MapBox* map, NodeType type = NodeType::MAP_NODE) noexcept;
    explicit Element(SequenceBox* sequence, NodeType type = NodeType::SEQUENCE_NODE) noexcept;

    Element(const Element& rhs) noexcept;
    Element(Element&& rhs) noexcept;

    Element& operator=(const Element& o) noexcept;
    Element& operator=(Element&& o) noexcept;

    ~Element() noexcept;

    inline NodeType type() const {
      return (NodeType) data_.index();
    }

    bool has(const Literal& key) const;

    template <typename T>
    const T as() const {
      assert(is_scalar());
      std::pair<bool, T> r = mhconfig::conversion::as<T>(data_);
      assert(r.first);
      return r.second;
    }

    template <typename T>
    const T as(T default_value) const {
      if (!is_scalar()) return default_value;
      std::pair<bool, T> r = mhconfig::conversion::as<T>(data_);
      if (!r.first) return default_value;
      return r.second;
    }

    template <typename T>
    const std::pair<bool, T> try_as() const {
      if (!is_scalar()) return std::make_pair(true, T());
      return mhconfig::conversion::as<T>(data_);
    }

    const Sequence* as_sequence() const;
    const Map* as_map() const;

    Element get(const Literal& key) const;
    Element get(size_t index) const;

    bool is_scalar() const;
    bool is_string() const;
    bool is_map() const;
    bool is_sequence() const;
    bool is_null() const;
    bool is_undefined() const;
    bool is_override() const;

    Element clone_without_virtual() const;

    std::string repr() const;

  private:
    Data data_;
  };

  namespace {
    inline void increment_refcount(Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::MAP_NODE:
          node_get<NodeType::MAP_NODE>(data)->increment_refcount();
          break;
        case NodeType::SEQUENCE_NODE:
          node_get<NodeType::SEQUENCE_NODE>(data)->increment_refcount();
          break;
        case NodeType::FORMAT_NODE:
          node_get<NodeType::FORMAT_NODE>(data)->increment_refcount();
          break;
        case NodeType::SREF_NODE:
          node_get<NodeType::SREF_NODE>(data)->increment_refcount();
          break;
        case NodeType::REF_NODE:
          node_get<NodeType::REF_NODE>(data)->increment_refcount();
          break;
        case NodeType::OVERRIDE_MAP_NODE:
          node_get<NodeType::OVERRIDE_MAP_NODE>(data)->increment_refcount();
          break;
        case NodeType::OVERRIDE_SEQUENCE_NODE:
          node_get<NodeType::OVERRIDE_SEQUENCE_NODE>(data)->increment_refcount();
          break;
        default:
          break;
      }
    }

    inline void decrement_refcount(Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::MAP_NODE:
          node_get<NodeType::MAP_NODE>(data)->decrement_refcount();
          break;
        case NodeType::SEQUENCE_NODE:
          node_get<NodeType::SEQUENCE_NODE>(data)->decrement_refcount();
          break;
        case NodeType::FORMAT_NODE:
          node_get<NodeType::FORMAT_NODE>(data)->decrement_refcount();
          break;
        case NodeType::SREF_NODE:
          node_get<NodeType::SREF_NODE>(data)->decrement_refcount();
          break;
        case NodeType::REF_NODE:
          node_get<NodeType::REF_NODE>(data)->decrement_refcount();
          break;
        case NodeType::OVERRIDE_MAP_NODE:
          node_get<NodeType::OVERRIDE_MAP_NODE>(data)->decrement_refcount();
          break;
        case NodeType::OVERRIDE_SEQUENCE_NODE:
          node_get<NodeType::OVERRIDE_SEQUENCE_NODE>(data)->decrement_refcount();
          break;
        default:
          break;
      }
    }
  }

  const static Element UNDEFINED_ELEMENT;
}

#endif
