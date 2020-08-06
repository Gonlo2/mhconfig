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
#include "jmutils/cow.h"
#include <fmt/format.h>
#include <boost/algorithm/string.hpp>

#include <absl/container/flat_hash_map.h>

namespace mhconfig {
  enum class NodeType : uint8_t {
    UNDEFINED,
    MAP,
    SEQUENCE,
    NONE,
    STR,
    BIN,
    INT,
    DOUBLE,
    BOOL,

    // Virtual nodes
    FORMAT,  // Sequence
    SREF,  // Sequence
    REF,  // Sequence
    DELETE, // None
    OVERRIDE_MAP, // Map
    OVERRIDE_SEQUENCE, // Sequence
    OVERRIDE_NONE, // None
    OVERRIDE_STR // Str
  };

  enum class InternalDataType {
    EMPTY,
    MAP,
    SEQUENCE,
    LITERAL,
    INT64,
    DOUBLE,
    BOOL
  };

  constexpr InternalDataType get_internal_data_type(NodeType type) {
    switch (type) {
      case NodeType::MAP: // Fallback
      case NodeType::OVERRIDE_MAP:
        return InternalDataType::MAP;
      case NodeType::SEQUENCE: // Fallback
      case NodeType::OVERRIDE_SEQUENCE: // Fallback
      case NodeType::FORMAT: // Fallback
      case NodeType::SREF: // Fallback
      case NodeType::REF:
        return InternalDataType::SEQUENCE;
      case NodeType::STR: // Fallback
      case NodeType::OVERRIDE_STR: // Fallback
      case NodeType::BIN:
        return InternalDataType::LITERAL;
      case NodeType::UNDEFINED: // Fallback
      case NodeType::NONE: // Fallback
      case NodeType::OVERRIDE_NONE: // Fallback
      case NodeType::DELETE:
        return InternalDataType::EMPTY;
      case NodeType::INT:
        return InternalDataType::INT64;
      case NodeType::DOUBLE:
        return InternalDataType::DOUBLE;
      case NodeType::BOOL:
        return InternalDataType::BOOL;
    }

    assert(false);
  }

  class Element;

  typedef jmutils::string::String Literal;
  typedef absl::flat_hash_map<jmutils::string::String, Element> Map;
  typedef std::vector<Element> Sequence;
  typedef jmutils::Cow<Map> MapCow;
  typedef jmutils::Cow<Sequence> SequenceCow;

  union data_t {
    MapCow map;
    SequenceCow seq;
    Literal literal;
    int64_t int64_value;
    double double_value;
    bool bool_value;

    data_t() noexcept {}
    ~data_t() noexcept {}
  };

  namespace conversion
  {
    template <typename T>
    std::pair<bool, T> as(NodeType type, const data_t& data);
  } /* conversion */

  std::string to_string(NodeType type);

  NodeType scalar_type(const Literal& tag);

  class Element final
  {
  public:
    Element() noexcept;
    explicit Element(NodeType type) noexcept;
    explicit Element(const int64_t value) noexcept;
    explicit Element(const double value) noexcept;
    explicit Element(const bool value) noexcept;

    explicit Element(const Literal& value, NodeType type = NodeType::STR) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::LITERAL);
      type_ = type;
      new (&data_.literal) Literal();
      data_.literal = value;
    }

    explicit Element(Literal&& value, NodeType type = NodeType::STR) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::LITERAL);
      type_ = type;
      new (&data_.literal) Literal();
      data_.literal = value;
    }

    explicit Element(const MapCow& map, NodeType type = NodeType::MAP) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::MAP);
      type_ = type;
      new (&data_.map) MapCow();
      data_.map = map;
    }

    explicit Element(MapCow&& map, NodeType type = NodeType::MAP) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::MAP);
      type_ = type;
      new (&data_.map) MapCow();
      data_.map = map;
    }

    explicit Element(const SequenceCow& sequence, NodeType type = NodeType::SEQUENCE) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::SEQUENCE);
      type_ = type;
      new (&data_.seq) SequenceCow();
      data_.seq = sequence;
    }

    explicit Element(SequenceCow&& sequence, NodeType type = NodeType::SEQUENCE) noexcept {
      assert(get_internal_data_type(type) == InternalDataType::SEQUENCE);
      type_ = type;
      new (&data_.seq) SequenceCow();
      data_.seq = sequence;
    }

    Element(const Element& rhs) noexcept;
    Element(Element&& rhs) noexcept;

    Element& operator=(const Element& o) noexcept;
    Element& operator=(Element&& o) noexcept;

    ~Element() noexcept;

    inline NodeType type() const {
      return type_;
    }

    bool has(const Literal& key) const;

    template <typename T>
    const T as() const {
      assert(is_scalar());
      std::pair<bool, T> r = conversion::as<T>(type_, data_);
      assert(r.first);
      return r.second;
    }

    template <typename T>
    const T as(T default_value) const {
      if (!is_scalar()) return default_value;
      std::pair<bool, T> r = conversion::as<T>(type_, data_);
      if (!r.first) return default_value;
      return r.second;
    }

    template <typename T>
    const std::pair<bool, T> try_as() const {
      if (!is_scalar()) return std::make_pair(true, T());
      return conversion::as<T>(type_, data_);
    }

    const Sequence* as_sequence() const;
    const Map* as_map() const;

    Sequence* as_sequence_mut();
    Map* as_map_mut();

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

    void freeze();

  private:
    NodeType type_;
    data_t data_;

    void init_data(NodeType type) noexcept;
    void destroy_data() noexcept;

    void copy_data(const Element& o) noexcept;
    void swap_data(Element& o) noexcept;

    void copy(const Element& o) noexcept;
    void swap(Element&& o) noexcept;
  };

  //TODO Check this
  const static Element UNDEFINED_ELEMENT;
}

#endif
