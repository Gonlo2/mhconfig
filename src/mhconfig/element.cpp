#include "mhconfig/element.h"

namespace mhconfig {
  std::string to_string(NodeType type) {
    switch (type) {
      case UNDEFINED_NODE: return "UNDEFINED";
      case MAP_NODE: return "MAP";
      case SEQUENCE_NODE: return "SEQUENCE";
      case NULL_NODE: return "NULL";
      case STR_NODE: return "STRING";
      case INT_NODE: return "INTEGER";
      case FLOAT_NODE: return "FLOAT";
      case BOOL_NODE: return "BOOLEAN";

      case FORMAT_NODE: return "FORMAT_NODE";
      case SREF_NODE: return "SREF_NODE";
      case REF_NODE: return "REF_NODE";
      case DELETE_NODE: return "DELETE_NODE";
      case OVERRIDE_MAP_NODE: return "OVERRIDE_MAP_NODE";
      case OVERRIDE_SEQUENCE_NODE: return "OVERRIDE_SEQUENCE_NODE";
      case OVERRIDE_NULL_NODE: return "OVERRIDE_NULL_NODE";
      case OVERRIDE_STR_NODE: return "OVERRIDE_STR_NODE";
      case OVERRIDE_INT_NODE: return "OVERRIDE_INT_NODE";
      case OVERRIDE_FLOAT_NODE: return "OVERRIDE_FLOAT_NODE";
      case OVERRIDE_BOOL_NODE: return "OVERRIDE_BOOL_NODE";
    }

    return "unknown";
  }

  Element::Element() noexcept {
  }

  Element::Element(NodeType type) noexcept {
    switch (type) {
      case UNDEFINED_NODE:
        data_.emplace<UNDEFINED_NODE>();
        break;
      case MAP_NODE:
        data_.emplace<MAP_NODE>();
        break;
      case SEQUENCE_NODE:
        data_.emplace<SEQUENCE_NODE>();
        break;
      case NULL_NODE:
        data_.emplace<NULL_NODE>();
        break;
      case STR_NODE:
        data_.emplace<STR_NODE>();
        break;
      case INT_NODE:
        data_.emplace<INT_NODE>();
        break;
      case FLOAT_NODE:
        data_.emplace<FLOAT_NODE>();
        break;
      case BOOL_NODE:
        data_.emplace<BOOL_NODE>();
        break;
      case FORMAT_NODE:
        data_.emplace<FORMAT_NODE>();
        break;
      case SREF_NODE:
        data_.emplace<SREF_NODE>();
        break;
      case REF_NODE:
        data_.emplace<REF_NODE>();
        break;
      case DELETE_NODE:
        data_.emplace<DELETE_NODE>();
        break;
      case OVERRIDE_MAP_NODE:
        data_.emplace<OVERRIDE_MAP_NODE>();
        break;
      case OVERRIDE_SEQUENCE_NODE:
        data_.emplace<OVERRIDE_SEQUENCE_NODE>();
        break;
      case OVERRIDE_NULL_NODE:
        data_.emplace<OVERRIDE_NULL_NODE>();
        break;
      case OVERRIDE_STR_NODE:
        data_.emplace<OVERRIDE_STR_NODE>();
        break;
      case OVERRIDE_INT_NODE:
        data_.emplace<OVERRIDE_INT_NODE>();
        break;
      case OVERRIDE_FLOAT_NODE:
        data_.emplace<OVERRIDE_FLOAT_NODE>();
        break;
      case OVERRIDE_BOOL_NODE:
        data_.emplace<OVERRIDE_BOOL_NODE>();
        break;
    }
  }

  Element::Element(const Literal& value, bool override_) noexcept {
    if (override_) {
      data_.emplace<OVERRIDE_STR_NODE>(value);
    } else {
      data_.emplace<STR_NODE>(value);
    }
  }

  Element::Element(int64_t value, bool override_) noexcept {
    if (override_) {
      data_.emplace<OVERRIDE_INT_NODE>(value);
    } else {
      data_.emplace<INT_NODE>(value);
    }
  }

  Element::Element(double value, bool override_) noexcept {
    if (override_) {
      data_.emplace<OVERRIDE_FLOAT_NODE>(value);
    } else {
      data_.emplace<FLOAT_NODE>(value);
    }
  }

  Element::Element(bool value, bool override_) noexcept {
    if (override_) {
      data_.emplace<OVERRIDE_BOOL_NODE>(value);
    } else {
      data_.emplace<BOOL_NODE>(value);
    }
  }

  Element::Element(MapBox* map, NodeType type) noexcept {
    map->increment_refcount();
    switch (type) {
      case MAP_NODE:
        data_.emplace<MAP_NODE>(map);
        break;
      case OVERRIDE_MAP_NODE:
        data_.emplace<OVERRIDE_MAP_NODE>(map);
        break;
      default:
        assert(false);
        break;
    }
  }

  Element::Element(SequenceBox* sequence, NodeType type) noexcept {
    sequence->increment_refcount();
    switch (type) {
      case SEQUENCE_NODE:
        data_.emplace<SEQUENCE_NODE>(sequence);
        break;
      case FORMAT_NODE:
        data_.emplace<FORMAT_NODE>(sequence);
        break;
      case SREF_NODE:
        data_.emplace<SREF_NODE>(sequence);
        break;
      case REF_NODE:
        data_.emplace<REF_NODE>(sequence);
        break;
      case OVERRIDE_SEQUENCE_NODE:
        data_.emplace<OVERRIDE_SEQUENCE_NODE>(sequence);
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
      case SEQUENCE_NODE:
        return std::get<SEQUENCE_NODE>(data_)->get();
      case FORMAT_NODE:
        return std::get<FORMAT_NODE>(data_)->get();
      case SREF_NODE:
        return std::get<SREF_NODE>(data_)->get();
      case REF_NODE:
        return std::get<REF_NODE>(data_)->get();
      case OVERRIDE_SEQUENCE_NODE:
        return std::get<OVERRIDE_SEQUENCE_NODE>(data_)->get();
      default:
        return nullptr;
    }
  }

  const Map* Element::as_map() const {
    switch (type()) {
      case MAP_NODE:
        return std::get<MAP_NODE>(data_)->get();
      case OVERRIDE_MAP_NODE:
        return std::get<OVERRIDE_MAP_NODE>(data_)->get();
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
      case STR_NODE: // Fallback
      case INT_NODE: // Fallback
      case FLOAT_NODE: // Fallback
      case BOOL_NODE: // Fallback
      case OVERRIDE_STR_NODE: // Fallback
      case OVERRIDE_INT_NODE: // Fallback
      case OVERRIDE_FLOAT_NODE: // Fallback
      case OVERRIDE_BOOL_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_string() const {
    switch (type()) {
      case STR_NODE: // Fallback
      case OVERRIDE_STR_NODE: // Fallback
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
      case NULL_NODE: // Fallback
      case OVERRIDE_NULL_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_undefined() const {
    return type() == UNDEFINED_NODE;
  }

  bool Element::is_override() const {
    switch (type()) {
      case OVERRIDE_MAP_NODE: // Fallback
      case OVERRIDE_SEQUENCE_NODE: // Fallback
      case OVERRIDE_NULL_NODE: // Fallback
      case OVERRIDE_STR_NODE: // Fallback
      case OVERRIDE_INT_NODE: // Fallback
      case OVERRIDE_FLOAT_NODE: // Fallback
      case OVERRIDE_BOOL_NODE:
        return true;
      default:
        break;
    }
    return false;
  }

  Element Element::clone_without_virtual() const {
    switch (type()) {
      case UNDEFINED_NODE:
        return Element();
      case MAP_NODE:
        return Element(std::get<MAP_NODE>(data_));
      case SEQUENCE_NODE:
        return Element(std::get<SEQUENCE_NODE>(data_));
      case NULL_NODE:
        return Element(NULL_NODE);
      case STR_NODE:
        return Element(std::get<STR_NODE>(data_));
      case INT_NODE:
        return Element(std::get<INT_NODE>(data_));
      case FLOAT_NODE:
        return Element(std::get<FLOAT_NODE>(data_));
      case BOOL_NODE:
        return Element(std::get<BOOL_NODE>(data_));
      case FORMAT_NODE:
        return Element(std::get<FORMAT_NODE>(data_));
      case SREF_NODE:
        return Element(std::get<SREF_NODE>(data_));
      case REF_NODE:
        return Element(std::get<REF_NODE>(data_));
      case DELETE_NODE:
        return Element();
      case OVERRIDE_MAP_NODE:
        return Element(std::get<OVERRIDE_MAP_NODE>(data_));
      case OVERRIDE_SEQUENCE_NODE:
        return Element(std::get<OVERRIDE_SEQUENCE_NODE>(data_));
      case OVERRIDE_NULL_NODE:
        return Element(OVERRIDE_NULL_NODE);
      case OVERRIDE_STR_NODE:
        return Element(std::get<OVERRIDE_STR_NODE>(data_));
      case OVERRIDE_INT_NODE:
        return Element(std::get<OVERRIDE_INT_NODE>(data_));
      case OVERRIDE_FLOAT_NODE:
        return Element(std::get<OVERRIDE_FLOAT_NODE>(data_));
      case OVERRIDE_BOOL_NODE:
        return Element(std::get<OVERRIDE_BOOL_NODE>(data_));
    }
    assert(false);
  }

  std::string Element::repr() const {
    std::stringstream ss;

    ss << "Element(";
    ss << "type: " << to_string(type());

    switch (type()) {
      case NULL_NODE: break;
      case UNDEFINED_NODE: break;

      case MAP_NODE: // Fallback
      case OVERRIDE_MAP_NODE:
        ss << ", size: " << as_map()->size();
        break;

      case SEQUENCE_NODE: // Fallback
      case FORMAT_NODE: // Fallback
      case SREF_NODE: // Fallback
      case REF_NODE: // Fallback
      case OVERRIDE_SEQUENCE_NODE:
        ss << ", size: " << as_sequence()->size();
        break;

      case STR_NODE:
        ss << ", str: '" << std::get<STR_NODE>(data_).str();
        break;
      case INT_NODE:
        ss << ", int: '" << std::get<INT_NODE>(data_);
        break;
      case FLOAT_NODE:
        ss << ", float: '" << std::get<FLOAT_NODE>(data_);
        break;
      case BOOL_NODE:
        ss << ", bool: '" << std::get<BOOL_NODE>(data_);
        break;

      case OVERRIDE_STR_NODE:
        ss << ", str: '" << std::get<OVERRIDE_STR_NODE>(data_).str();
        break;
      case OVERRIDE_INT_NODE:
        ss << ", int: '" << std::get<OVERRIDE_INT_NODE>(data_);
        break;
      case OVERRIDE_FLOAT_NODE:
        ss << ", float: '" << std::get<OVERRIDE_FLOAT_NODE>(data_);
        break;
      case OVERRIDE_BOOL_NODE:
        ss << ", bool: '" << std::get<OVERRIDE_BOOL_NODE>(data_);
        break;
    }

    ss << "')";

    return ss.str();
  }

  namespace conversion
  {
    template <>
    std::pair<bool, ::string_pool::String> as<::string_pool::String>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::STR_NODE:
          return std::make_pair(true, std::get<STR_NODE>(data));
        case NodeType::OVERRIDE_STR_NODE:
          return std::make_pair(true, std::get<OVERRIDE_STR_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, ::string_pool::String());
    }

    template <>
    std::pair<bool, std::string> as<std::string>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::STR_NODE:
          return std::make_pair(true, std::get<STR_NODE>(data).str());
        case NodeType::OVERRIDE_STR_NODE:
          return std::make_pair(true, std::get<OVERRIDE_STR_NODE>(data).str());
        case NodeType::BOOL_NODE:
          return std::make_pair(true, std::get<BOOL_NODE>(data) ? "true" : "false");
        case NodeType::OVERRIDE_BOOL_NODE:
          return std::make_pair(true, std::get<OVERRIDE_BOOL_NODE>(data) ? "true" : "false");
        case NodeType::INT_NODE:
          return std::make_pair(true, std::to_string(std::get<INT_NODE>(data)));
        case NodeType::OVERRIDE_INT_NODE:
          return std::make_pair(true, std::to_string(std::get<OVERRIDE_INT_NODE>(data)));
        case NodeType::FLOAT_NODE:
          return std::make_pair(true, std::to_string(std::get<FLOAT_NODE>(data)));
        case NodeType::OVERRIDE_FLOAT_NODE:
          return std::make_pair(true, std::to_string(std::get<OVERRIDE_FLOAT_NODE>(data)));
        default:
          break;
      }
      return std::make_pair(false, "");
    }

    template <>
    std::pair<bool, bool> as<bool>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::BOOL_NODE:
          return std::make_pair(true, std::get<BOOL_NODE>(data));
        case NodeType::OVERRIDE_BOOL_NODE:
          return std::make_pair(true, std::get<OVERRIDE_BOOL_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, false);
    }

    template <>
    std::pair<bool, int64_t> as<int64_t>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::INT_NODE:
          return std::make_pair(true, std::get<INT_NODE>(data));
        case NodeType::OVERRIDE_INT_NODE:
          return std::make_pair(true, std::get<OVERRIDE_INT_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, 0);
    }

    template <>
    std::pair<bool, double> as<double>(const Data& data) {
      switch ((NodeType) data.index()) {
        case NodeType::FLOAT_NODE:
          return std::make_pair(true, std::get<FLOAT_NODE>(data));
        case NodeType::OVERRIDE_FLOAT_NODE:
          return std::make_pair(true, std::get<OVERRIDE_FLOAT_NODE>(data));
        default:
          break;
      }
      return std::make_pair(false, 0.0);
    }

  } /* conversion */
}
