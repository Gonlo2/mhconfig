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
#include "mhconfig/constants.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"
#include <fmt/format.h>

#define bitf_swap(a, b) { auto tmp = a; a = b; b = tmp; }

namespace mhconfig {
  class Element;

  typedef jmutils::string::String Literal;
  typedef absl::flat_hash_map<jmutils::string::String, Element> Map;
  typedef std::vector<Element> Seq;

  namespace conversion
  {
    template <typename T>
    std::optional<T> as(const Element& e);
  } /* conversion */

  class Element final
  {
  public:
    enum class Type: uint8_t {
      UNDEFINED = 0,
      MAP       = 1,
      SEQUENCE  = 2,
      NONE      = 3,
      STR       = 4,
      BIN       = 5,
      INT64     = 6,
      DOUBLE    = 7,
      BOOL      = 8,
    };

    enum class Tag: uint8_t {
      NONE       = 0,
      FORMAT     = 1,
      SREF       = 2,
      REF        = 3,
      DELETE     = 4,
      OVERRIDE   = 5,
    };

    Element() noexcept;
    explicit Element(Type type) noexcept;
    explicit Element(int64_t value) noexcept;
    explicit Element(double value) noexcept;
    explicit Element(bool value) noexcept;

    explicit Element(const Literal& value, Type type = Type::STR) noexcept;
    explicit Element(Literal&& value, Type type = Type::STR) noexcept;

    explicit Element(const Map& map) noexcept;
    explicit Element(Map&& map) noexcept;

    explicit Element(const Seq& seq) noexcept;
    explicit Element(Seq&& seq) noexcept;

    Element(const Element& rhs) noexcept;
    Element(Element&& rhs) noexcept;

    Element& operator=(const Element& o) noexcept;
    Element& operator=(Element&& o) noexcept;

    ~Element() noexcept;

    inline Type type() const {
      return type_;
    }

    inline void set_tag(Tag tag) {
      tag_ = tag;
    }

    inline Tag tag() const {
      return tag_;
    }

    inline void set_document_id(DocumentId document_id) {
      document_id_ = document_id;
    }

    inline DocumentId document_id() const {
      return document_id_;
    }

    inline void set_raw_config_id(RawConfigId raw_config_id) {
      raw_config_id_ = raw_config_id;
    }

    inline RawConfigId raw_config_id() const {
      return raw_config_id_;
    }

    inline void set_position(int line, int col) {
      line_ = (0 <= line) && (line <= 0xffff) ? line : 0;
      col_ = (0 <= col) && (col <= 0xff) ? col : 0;
    }

    inline uint16_t line() const {
      return line_;
    }

    inline uint8_t col() const {
      return col_;
    }

    inline Element& set_origin(const Element& o) {
      line_ = o.line_;
      col_ = o.col_;
      document_id_ = o.document_id_;
      raw_config_id_ = o.raw_config_id_;
      return *this;
    }

    bool has(const Literal& key) const;

    template <typename T>
    const T as() const {
      assert(is_scalar());
      auto r = conversion::as<T>(*this);
      assert(r);
      return *r;
    }

    template <typename T>
    const T as(T default_value) const {
      if (!is_scalar()) return default_value;
      auto r = conversion::as<T>(*this);
      return r ? *r : default_value;
    }

    template <typename T>
    const std::optional<T> try_as() const {
      return conversion::as<T>(*this);
    }

    const Seq* as_seq() const;
    const Map* as_map() const;

    Seq* as_seq_mut();
    Map* as_map_mut();

    Element get(const std::string& key) const;
    Element get(const Literal& key) const;
    Element get(size_t index) const;

    bool is_scalar() const;
    bool is_string() const;
    bool is_map() const;
    bool is_seq() const;
    bool is_null() const;
    bool is_undefined() const;
    bool is_override() const;

    void freeze();

    template <typename F>
    void walk_mut(F lambda) {
      lambda(this);

      switch (type_) {
        case Type::MAP:
          for (auto& it : *as_map_mut()) {
            it.second.walk_mut(lambda);
          }
          break;
        case Type::SEQUENCE:
          for (auto& e : *as_seq_mut()) {
            e.walk_mut(lambda);
          }
          break;
        default:
          break;
      }
    }

    std::array<uint8_t, 32> make_checksum() const;

    std::optional<std::string> to_yaml() const;

  private:
    template <typename T>
    friend std::optional<T> conversion::as(const Element& e);

    typedef jmutils::Cow<Map> MapData;
    typedef jmutils::Cow<Seq> SeqData;

    union data_t {
      MapData map;
      SeqData seq;
      Literal literal;

      int64_t int64_value;
      double double_value;
      bool bool_value;

      // This is used to create the fingerprint
      uint64_t uint64_value;

      data_t() noexcept {}
      ~data_t() noexcept {}
    };

    Type type_ : 4;
    Tag tag_ : 4;
    uint8_t col_;
    uint16_t line_;
    DocumentId document_id_;
    RawConfigId raw_config_id_;
    data_t data_;

    void init(Type type) noexcept;
    void init_data(Type type) noexcept;
    void destroy_data() noexcept;

    void copy(const Element& o) noexcept;
    void swap(Element& o) noexcept;

    void add_fingerprint(std::string& output) const;

    bool to_yaml_base(YAML::Emitter& out) const;
  };

  //TODO Check this
  const static Element UNDEFINED_ELEMENT;
  const static std::array<uint8_t, 32> UNDEFINED_ELEMENT_CHECKSUM{UNDEFINED_ELEMENT.make_checksum()};

  std::string to_string(Element::Type type);
}

template <> struct fmt::formatter<mhconfig::Element>: formatter<string_view> {
  template <typename FormatContext>
  auto format(const mhconfig::Element& element, FormatContext& ctx) {
    auto oit = format_to(
      ctx.out(),
      "Element(type: {}, line: {}, col: {}",
      to_string(element.type()),
      element.line(),
      element.col()
    );

    switch (element.tag()) {
      case mhconfig::Element::Tag::NONE:
        break;
      case mhconfig::Element::Tag::FORMAT:
        oit = format_to(oit, ", tag: !format");
        break;
      case mhconfig::Element::Tag::SREF:
        oit = format_to(oit, ", tag: !sref");
        break;
      case mhconfig::Element::Tag::REF:
        oit = format_to(oit, ", tag: !ref");
        break;
      case mhconfig::Element::Tag::DELETE:
        oit = format_to(oit, ", tag: !delete");
        break;
      case mhconfig::Element::Tag::OVERRIDE:
        oit = format_to(oit, ", tag: !override");
        break;
    }

    switch (element.type()) {
      case mhconfig::Element::Type::MAP:
        oit = format_to(oit, ", size: {}", element.as_map()->size());
        break;
      case mhconfig::Element::Type::SEQUENCE:
        oit = format_to(oit, ", size: {}", element.as_seq()->size());
        break;
      case mhconfig::Element::Type::BIN:
        oit = format_to(oit, ", size: {}", element.as<jmutils::string::String>().size());
        break;
      case mhconfig::Element::Type::STR:
        oit = format_to(oit, ", str: '{}'", element.as<std::string>());
        break;
      case mhconfig::Element::Type::INT64:
        oit = format_to(oit, ", int64: {}", element.as<int64_t>());
        break;
      case mhconfig::Element::Type::DOUBLE:
        oit = format_to(oit, ", double: {}", element.as<double>());
        break;
      case mhconfig::Element::Type::BOOL:
        oit = format_to(oit, ", bool: {}", element.as<bool>());
        break;
    }

    return format_to(oit, ")");
  }
};

#endif
