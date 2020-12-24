#ifndef MHCONFIG__ELEMENT_H
#define MHCONFIG__ELEMENT_H

#include <absl/container/flat_hash_map.h>
#include <assert.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <openssl/sha.h>
#include <stddef.h>
#include <array>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "jmutils/common.h"
#include "jmutils/cow.h"
#include "jmutils/string/pool.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#include <fmt/format.h>

namespace mhconfig {
  enum class NodeType : uint8_t {
    UNDEFINED         = 0,
    MAP               = 1,
    SEQUENCE          = 2,
    NONE              = 3,
    STR               = 4,
    BIN               = 5,
    INT64             = 6,
    DOUBLE            = 7,
    BOOL              = 8,

    // Virtual nodes
    FORMAT            = 9,  // Sequence
    SREF              = 10, // Sequence
    REF               = 11, // Sequence
    DELETE            = 12, // None
    OVERRIDE_MAP      = 13, // Map
    OVERRIDE_SEQUENCE = 14, // Sequence
    OVERRIDE_NONE     = 15, // None
    OVERRIDE_STR      = 16  // Str
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
      case NodeType::INT64:
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

    // This is used to create the fingerprint
    uint64_t uint64_value;

    data_t() noexcept {}
    ~data_t() noexcept {}
  };

  namespace conversion
  {
    template <typename T>
    std::optional<T> as(NodeType type, const data_t& data);
  } /* conversion */

  std::string to_string(NodeType type);

  NodeType scalar_type(const Literal& tag);

  class Element final
  {
  public:
    Element() noexcept;
    explicit Element(NodeType type) noexcept;
    explicit Element(int64_t value) noexcept;
    explicit Element(double value) noexcept;
    explicit Element(bool value) noexcept;

    explicit Element(const Literal& value, NodeType type = NodeType::STR) noexcept;
    explicit Element(Literal&& value, NodeType type = NodeType::STR) noexcept;

    explicit Element(const MapCow& map, NodeType type = NodeType::MAP) noexcept;
    explicit Element(MapCow&& map, NodeType type = NodeType::MAP) noexcept;

    explicit Element(const SequenceCow& sequence, NodeType type = NodeType::SEQUENCE) noexcept;
    explicit Element(SequenceCow&& sequence, NodeType type = NodeType::SEQUENCE) noexcept;

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
      auto r = conversion::as<T>(type_, data_);
      assert(r);
      return *r;
    }

    template <typename T>
    const T as(T default_value) const {
      if (!is_scalar()) return default_value;
      auto r = conversion::as<T>(type_, data_);
      return r ? *r : default_value;
    }

    template <typename T>
    const std::optional<T> try_as() const {
      return conversion::as<T>(type_, data_);
    }

    const Sequence* as_sequence() const;
    const Map* as_map() const;

    Sequence* as_sequence_mut();
    Map* as_map_mut();

    Element get(const std::string& key) const;
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

    void freeze();

    std::array<uint8_t, 32> make_checksum() const;

    std::optional<std::string> to_yaml() const;

  private:
    NodeType type_;
    data_t data_;

    void init_data(NodeType type) noexcept;
    void destroy_data() noexcept;

    void copy_data(const Element& o) noexcept;
    void swap_data(Element& o) noexcept;

    void copy(const Element& o) noexcept;
    void swap(Element&& o) noexcept;

    void add_fingerprint(std::string& output) const;

    bool to_yaml_base(YAML::Emitter& out) const;
  };

  //TODO Check this
  const static Element UNDEFINED_ELEMENT;
}


template <> struct fmt::formatter<mhconfig::Element>: formatter<string_view> {
  template <typename FormatContext>
  auto format(const mhconfig::Element& element, FormatContext& ctx) {
    auto oit = format_to(
      ctx.out(),
      "Element(type: {}",
      to_string(element.type())
    );

    switch (get_internal_data_type(element.type())) {
      case mhconfig::InternalDataType::EMPTY:
        break;
      case mhconfig::InternalDataType::MAP:
        oit = format_to(oit, ", size: {}", element.as_map()->size());
        break;
      case mhconfig::InternalDataType::SEQUENCE:
        oit = format_to(oit, ", size: {}", element.as_sequence()->size());
        break;
      case mhconfig::InternalDataType::LITERAL:
        oit = format_to(oit, ", literal: '{}'", element.as<std::string>());
        break;
      case mhconfig::InternalDataType::INT64:
        oit = format_to(oit, ", int64: {}", element.as<int64_t>());
        break;
      case mhconfig::InternalDataType::DOUBLE:
        oit = format_to(oit, ", double: {}", element.as<double>());
        break;
      case mhconfig::InternalDataType::BOOL:
        oit = format_to(oit, ", bool: {}", element.as<bool>());
        break;
    }

    return format_to(oit, ")");
  }
};

#endif
