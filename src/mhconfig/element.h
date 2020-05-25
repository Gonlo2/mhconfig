#ifndef MHCONFIG__ELEMENT_H
#define MHCONFIG__ELEMENT_H

#include <unordered_map>
#include <vector>
#include <utility>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>

#include "yaml-cpp/yaml.h"
#include "spdlog/spdlog.h"
#include "string_pool/pool.h"
#include <fmt/format.h>
#include <boost/algorithm/string.hpp>

namespace mhconfig {
  enum NodeType {
    UNDEFINED_NODE = 0,
    MAP_NODE = 1,
    SEQUENCE_NODE = 2,
    NULL_NODE = 3,
    SCALAR_NODE = 4,
  };

  std::string to_string(NodeType type);

  class Element;

  typedef std::shared_ptr<Element> ElementRef;
  typedef ::string_pool::String Literal;
  typedef std::unordered_map<::string_pool::String, ElementRef> Map;
  typedef std::shared_ptr<Map> MapRef;
  typedef std::vector<ElementRef> Sequence;
  typedef std::shared_ptr<Sequence> SequenceRef;

  const static ElementRef UNDEFINED_ELEMENT{std::make_shared<Element>()};

  const static std::string DATA_TAG_EMPTY{"?"};
  const static Literal TAG_EMPTY{::string_pool::make_small_string(DATA_TAG_EMPTY)};

  namespace conversion
  {
    template <typename T>
    std::pair<bool, T> as(NodeType type, const Literal& literal);
  } /* conversion */

  class Element final
  {
  public:
    Element();
    Element(NodeType type);
    Element(const Literal& literal);
    Element(const Literal& literal, const Literal& tag);
    Element(const MapRef map);
    Element(const MapRef map, const Literal& tag);
    Element(const SequenceRef sequence);
    Element(const SequenceRef sequence, const Literal& tag);

    Element(const Element& rhs) = delete;
    Element(Element&& rhs) = delete;

    ~Element() {};

    NodeType type() const;
    const Literal& tag() const;

    bool has(const Literal& key) const;

    template <typename T>
    const T as() const {
      assert(type_ != UNDEFINED_NODE);

      std::pair<bool, T> r = mhconfig::conversion::as<T>(type_, literal_);
      assert(r.first);
      return r.second;
    }

    template <typename T>
    const T as(T default_value) const {
      if (type_ == UNDEFINED_NODE) return default_value;

      std::pair<bool, T> r = mhconfig::conversion::as<T>(type_, literal_);
      if (!r.first) return default_value;
      return r.second;
    }

    const Sequence& as_sequence() const;
    const Map& as_map() const;

    ElementRef get(const Literal& key) const;
    ElementRef get(size_t index) const;

    bool is_scalar() const;
    bool is_map() const;
    bool is_sequence() const;
    bool is_null() const;
    bool is_undefined() const;

    std::pair<bool, std::string> to_yaml() const;

    ElementRef clone_without_tag() const;

    std::string repr() const;

  private:
    NodeType type_;
    const Literal tag_;

    const Literal literal_;
    const MapRef map_{nullptr};
    const SequenceRef sequence_{nullptr};

    bool to_yaml_base(YAML::Emitter& out) const;
  };
}

#endif
