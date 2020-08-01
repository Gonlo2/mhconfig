#include "mhconfig/element.h"

namespace mhconfig {
  std::string to_string(NodeType type) {
    switch (type) {
      case NodeType::UNDEFINED_NODE:
        return "UNDEFINED";
      case NodeType::MAP_NODE:
        return "MAP";
      case NodeType::SEQUENCE_NODE:
        return "SEQUENCE";
      case NodeType::NULL_NODE:
        return "NULL";
      case NodeType::STR_NODE:
        return "STRING";
      case NodeType::BIN_NODE:
        return "STRING";
      case NodeType::INT_NODE:
        return "INTEGER";
      case NodeType::FLOAT_NODE:
        return "FLOAT";
      case NodeType::BOOL_NODE:
        return "BOOLEAN";

      case NodeType::FORMAT_NODE:
        return "FORMAT_NODE";
      case NodeType::SREF_NODE:
        return "SREF_NODE";
      case NodeType::REF_NODE:
        return "REF_NODE";
      case NodeType::DELETE_NODE:
        return "DELETE_NODE";
      case NodeType::OVERRIDE_MAP_NODE:
        return "OVERRIDE_MAP_NODE";
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        return "OVERRIDE_SEQUENCE_NODE";
      case NodeType::OVERRIDE_NULL_NODE:
        return "OVERRIDE_NULL_NODE";
      case NodeType::OVERRIDE_STR_NODE:
        return "OVERRIDE_STR_NODE";
    }

    return "unknown";
  }

  Element::Element() noexcept {
  }

  Element::Element(NodeType type) noexcept {
    switch (type) {
      case NodeType::UNDEFINED_NODE:
        node_set<NodeType::UNDEFINED_NODE>(data_);
        break;
      case NodeType::MAP_NODE:
        node_set<NodeType::MAP_NODE>(data_);
        break;
      case NodeType::SEQUENCE_NODE:
        node_set<NodeType::SEQUENCE_NODE>(data_);
        break;
      case NodeType::NULL_NODE:
        node_set<NodeType::NULL_NODE>(data_);
        break;
      case NodeType::STR_NODE:
        node_set<NodeType::STR_NODE>(data_);
        break;
      case NodeType::BIN_NODE:
        node_set<NodeType::BIN_NODE>(data_);
        break;
      case NodeType::INT_NODE:
        node_set<NodeType::INT_NODE>(data_);
        break;
      case NodeType::FLOAT_NODE:
        node_set<NodeType::FLOAT_NODE>(data_);
        break;
      case NodeType::BOOL_NODE:
        node_set<NodeType::BOOL_NODE>(data_);
        break;
      case NodeType::FORMAT_NODE:
        node_set<NodeType::FORMAT_NODE>(data_);
        break;
      case NodeType::SREF_NODE:
        node_set<NodeType::SREF_NODE>(data_);
        break;
      case NodeType::REF_NODE:
        node_set<NodeType::REF_NODE>(data_);
        break;
      case NodeType::DELETE_NODE:
        node_set<NodeType::DELETE_NODE>(data_);
        break;
      case NodeType::OVERRIDE_MAP_NODE:
        node_set<NodeType::OVERRIDE_MAP_NODE>(data_);
        break;
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        node_set<NodeType::OVERRIDE_SEQUENCE_NODE>(data_);
        break;
      case NodeType::OVERRIDE_NULL_NODE:
        node_set<NodeType::OVERRIDE_NULL_NODE>(data_);
        break;
      case NodeType::OVERRIDE_STR_NODE:
        node_set<NodeType::OVERRIDE_STR_NODE>(data_);
        break;
    }
  }

  Element::Element(const Literal& value, bool override_, bool is_binary) noexcept {
    if (is_binary) {
      node_set<NodeType::BIN_NODE>(data_, value);
    } else {
      if (override_) {
        node_set<NodeType::OVERRIDE_STR_NODE>(data_, value);
      } else {
        node_set<NodeType::STR_NODE>(data_, value);
      }
    }
  }

  Element::Element(int64_t value) noexcept {
    node_set<NodeType::INT_NODE>(data_, value);
  }

  Element::Element(double value) noexcept {
    node_set<NodeType::FLOAT_NODE>(data_, value);
  }

  Element::Element(bool value) noexcept {
    node_set<NodeType::BOOL_NODE>(data_, value);
  }

  Element::Element(MapBox* map, NodeType type) noexcept {
    map->increment_refcount();
    switch (type) {
      case NodeType::MAP_NODE:
        node_set<NodeType::MAP_NODE>(data_, map);
        break;
      case NodeType::OVERRIDE_MAP_NODE:
        node_set<NodeType::OVERRIDE_MAP_NODE>(data_, map);
        break;
      default:
        assert(false);
        break;
    }
  }

  Element::Element(SequenceBox* sequence, NodeType type) noexcept {
    sequence->increment_refcount();
    switch (type) {
      case NodeType::SEQUENCE_NODE:
        node_set<NodeType::SEQUENCE_NODE>(data_, sequence);
        break;
      case NodeType::FORMAT_NODE:
        node_set<NodeType::FORMAT_NODE>(data_, sequence);
        break;
      case NodeType::SREF_NODE:
        node_set<NodeType::SREF_NODE>(data_, sequence);
        break;
      case NodeType::REF_NODE:
        node_set<NodeType::REF_NODE>(data_, sequence);
        break;
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        node_set<NodeType::OVERRIDE_SEQUENCE_NODE>(data_, sequence);
        break;
      default:
        assert(false);
        break;
    }
  }

  Element::Element(const Element& rhs) noexcept : data_(rhs.data_) {
    increment_refcount(data_);
  }

  Element::Element(Element&& rhs) noexcept {
    std::swap(data_, rhs.data_);
  }

  Element& Element::operator=(const Element& o) noexcept {
    if (this == &o) return *this;
    decrement_refcount(data_);
    data_ = o.data_;
    increment_refcount(data_);
    return *this;
  }

  Element& Element::operator=(Element&& o) noexcept {
    std::swap(data_, o.data_);
    return *this;
  }

  Element::~Element() noexcept {
    decrement_refcount(data_);
  }

  const Sequence* Element::as_sequence() const {
    switch (type()) {
      case NodeType::SEQUENCE_NODE:
        return node_get<NodeType::SEQUENCE_NODE>(data_)->get();
      case NodeType::FORMAT_NODE:
        return node_get<NodeType::FORMAT_NODE>(data_)->get();
      case NodeType::SREF_NODE:
        return node_get<NodeType::SREF_NODE>(data_)->get();
      case NodeType::REF_NODE:
        return node_get<NodeType::REF_NODE>(data_)->get();
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        return node_get<NodeType::OVERRIDE_SEQUENCE_NODE>(data_)->get();
      default:
        return nullptr;
    }
  }

  const Map* Element::as_map() const {
    switch (type()) {
      case NodeType::MAP_NODE:
        return node_get<NodeType::MAP_NODE>(data_)->get();
      case NodeType::OVERRIDE_MAP_NODE:
        return node_get<NodeType::OVERRIDE_MAP_NODE>(data_)->get();
      default:
        return nullptr;
    }
  }

  Element Element::get(const Literal& key) const {
    auto map = as_map();
    if (map == nullptr) {
      spdlog::debug("The element {} isn't a map", repr());
      return Element();
    }

    auto search = map->find(key);
    return (search == map->end()) ? Element() : search->second;
  }

  Element Element::get(size_t index) const {
    auto seq = as_sequence();
    if (seq == nullptr) {
      spdlog::debug("The element {} isn't a sequence", repr());
      return Element();
    }

    return (index < seq->size()) ? (*seq)[index] : Element();
  }

  bool Element::has(const Literal& key) const {
    auto map = as_map();
    return (map == nullptr) ? false : map->count(key) > 0;
  }

  bool Element::is_scalar() const {
    switch (type()) {
      case NodeType::STR_NODE: // Fallback
      case NodeType::BIN_NODE: // Fallback
      case NodeType::INT_NODE: // Fallback
      case NodeType::FLOAT_NODE: // Fallback
      case NodeType::BOOL_NODE: // Fallback
      case NodeType::OVERRIDE_STR_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_string() const {
    switch (type()) {
      case NodeType::STR_NODE: // Fallback
      case NodeType::OVERRIDE_STR_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_map() const {
    return as_map() != nullptr;
  }

  bool Element::is_sequence() const {
    return as_sequence() != nullptr;
  }

  bool Element::is_null() const {
    switch (type()) {
      case NodeType::NULL_NODE: // Fallback
      case NodeType::OVERRIDE_NULL_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_undefined() const {
    return type() == NodeType::UNDEFINED_NODE;
  }

  bool Element::is_override() const {
    switch (type()) {
      case NodeType::OVERRIDE_MAP_NODE: // Fallback
      case NodeType::OVERRIDE_SEQUENCE_NODE: // Fallback
      case NodeType::OVERRIDE_NULL_NODE: // Fallback
      case NodeType::OVERRIDE_STR_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  Element Element::clone_without_virtual() const {
    switch (type()) {
      case NodeType::UNDEFINED_NODE:
        return Element();
      case NodeType::MAP_NODE:
        return Element(node_get<NodeType::MAP_NODE>(data_));
      case NodeType::SEQUENCE_NODE:
        return Element(node_get<NodeType::SEQUENCE_NODE>(data_));
      case NodeType::NULL_NODE:
        return Element(NodeType::NULL_NODE);
      case NodeType::STR_NODE:
        return Element(node_get<NodeType::STR_NODE>(data_));
      case NodeType::BIN_NODE:
        return Element(node_get<NodeType::BIN_NODE>(data_));
      case NodeType::INT_NODE:
        return Element(node_get<NodeType::INT_NODE>(data_));
      case NodeType::FLOAT_NODE:
        return Element(node_get<NodeType::FLOAT_NODE>(data_));
      case NodeType::BOOL_NODE:
        return Element(node_get<NodeType::BOOL_NODE>(data_));
      case NodeType::FORMAT_NODE:
        return Element(node_get<NodeType::FORMAT_NODE>(data_));
      case NodeType::SREF_NODE:
        return Element(node_get<NodeType::SREF_NODE>(data_));
      case NodeType::REF_NODE:
        return Element(node_get<NodeType::REF_NODE>(data_));
      case NodeType::DELETE_NODE:
        return Element();
      case NodeType::OVERRIDE_MAP_NODE:
        return Element(node_get<NodeType::OVERRIDE_MAP_NODE>(data_));
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        return Element(node_get<NodeType::OVERRIDE_SEQUENCE_NODE>(data_));
      case NodeType::OVERRIDE_NULL_NODE:
        return Element(NodeType::OVERRIDE_NULL_NODE);
      case NodeType::OVERRIDE_STR_NODE:
        return Element(node_get<NodeType::OVERRIDE_STR_NODE>(data_));
    }
    assert(false);
  }

  std::string Element::repr() const {
    std::stringstream ss;

    ss << "Element(";
    ss << "type: " << to_string(type());

    switch (type()) {
      case NodeType::NULL_NODE: break;
      case NodeType::UNDEFINED_NODE: break;

      case NodeType::MAP_NODE: // Fallback
      case NodeType::OVERRIDE_MAP_NODE:
        ss << ", size: " << as_map()->size();
        break;

      case NodeType::SEQUENCE_NODE: // Fallback
      case NodeType::FORMAT_NODE: // Fallback
      case NodeType::SREF_NODE: // Fallback
      case NodeType::REF_NODE: // Fallback
      case NodeType::OVERRIDE_SEQUENCE_NODE:
        ss << ", size: " << as_sequence()->size();
        break;

      case NodeType::STR_NODE:
        ss << ", str: '" << node_get<NodeType::STR_NODE>(data_).str();
        break;
      case NodeType::BIN_NODE:
        ss << ", bin: '" << node_get<NodeType::BIN_NODE>(data_).str();
        break;
      case NodeType::INT_NODE:
        ss << ", int: '" << node_get<NodeType::INT_NODE>(data_);
        break;
      case NodeType::FLOAT_NODE:
        ss << ", float: '" << node_get<NodeType::FLOAT_NODE>(data_);
        break;
      case NodeType::BOOL_NODE:
        ss << ", bool: '" << node_get<NodeType::BOOL_NODE>(data_);
        break;

      case NodeType::OVERRIDE_STR_NODE:
        ss << ", str: '" << node_get<NodeType::OVERRIDE_STR_NODE>(data_).str();
        break;
    }

    ss << "')";

    return ss.str();
  }

  namespace conversion
  {
    template <>
    std::pair<bool, jmutils::string::String> as<jmutils::string::String>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::STR_NODE:
          return std::make_pair(true, node_get<NodeType::STR_NODE>(data));
        case NodeType::BIN_NODE:
          return std::make_pair(true, node_get<NodeType::BIN_NODE>(data));
        case NodeType::OVERRIDE_STR_NODE:
          return std::make_pair(true, node_get<NodeType::OVERRIDE_STR_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, jmutils::string::String());
    }

    template <>
    std::pair<bool, std::string> as<std::string>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::STR_NODE:
          return std::make_pair(true, node_get<NodeType::STR_NODE>(data).str());
        case NodeType::BIN_NODE:
          return std::make_pair(true, node_get<NodeType::BIN_NODE>(data).str());
        case NodeType::OVERRIDE_STR_NODE:
          return std::make_pair(true, node_get<NodeType::OVERRIDE_STR_NODE>(data).str());

        case NodeType::BOOL_NODE:
          return std::make_pair(true, node_get<NodeType::BOOL_NODE>(data) ? "true" : "false");
        case NodeType::INT_NODE:
          return std::make_pair(true, std::to_string(node_get<NodeType::INT_NODE>(data)));
        case NodeType::FLOAT_NODE:
          return std::make_pair(true, std::to_string(node_get<NodeType::FLOAT_NODE>(data)));
        default:
          break;
      }
      return std::make_pair(false, "");
    }

    template <>
    std::pair<bool, bool> as<bool>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::BOOL_NODE:
          return std::make_pair(true, node_get<NodeType::BOOL_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, false);
    }

    template <>
    std::pair<bool, int64_t> as<int64_t>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::INT_NODE:
          return std::make_pair(true, node_get<NodeType::INT_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, 0);
    }

    template <>
    std::pair<bool, double> as<double>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::FLOAT_NODE:
          return std::make_pair(true, node_get<NodeType::FLOAT_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, 0.0);
    }

  } /* conversion */
}
