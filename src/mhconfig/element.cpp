#include "mhconfig/element.h"

namespace mhconfig {
  std::string to_string(NodeType type) {
    switch (type) {
      case NodeType::UNDEFINED:
        return "UNDEFINED";
      case NodeType::MAP:
        return "MAP";
      case NodeType::SEQUENCE:
        return "SEQUENCE";
      case NodeType::NONE:
        return "NONE";
      case NodeType::STR:
        return "STR";
      case NodeType::BIN:
        return "BIN";
      case NodeType::INT64:
        return "INT64";
      case NodeType::DOUBLE:
        return "DOUBLE";
      case NodeType::BOOL:
        return "BOOLEAN";

      case NodeType::FORMAT:
        return "FORMAT";
      case NodeType::SREF:
        return "SREF";
      case NodeType::REF:
        return "REF";
      case NodeType::DELETE:
        return "DELETE";
      case NodeType::OVERRIDE_MAP:
        return "OVERRIDE_MAP";
      case NodeType::OVERRIDE_SEQUENCE:
        return "OVERRIDE_SEQUENCE";
      case NodeType::OVERRIDE_NONE:
        return "OVERRIDE_NONE";
      case NodeType::OVERRIDE_STR:
        return "OVERRIDE_STR";
    }

    return "unknown";
  }

  Element::Element() noexcept {
    init_data(NodeType::UNDEFINED);
  }

  Element::Element(NodeType type) noexcept {
    init_data(type);
  }

  Element::Element(int64_t value) noexcept {
    type_ = NodeType::INT64;
    data_.int64_value = value;
  }

  Element::Element(double value) noexcept {
    type_ = NodeType::DOUBLE;
    data_.double_value = value;
  }

  Element::Element(bool value) noexcept {
    type_ = NodeType::BOOL;
    data_.bool_value = value;
  }

  Element::Element(const Literal& value, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::LITERAL);
    type_ = type;
    new (&data_.literal) Literal();
    data_.literal = value;
  }

  Element::Element(Literal&& value, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::LITERAL);
    type_ = type;
    new (&data_.literal) Literal();
    data_.literal = value;
  }

  Element::Element(const MapCow& map, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::MAP);
    type_ = type;
    new (&data_.map) MapCow();
    data_.map = map;
  }

  Element::Element(MapCow&& map, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::MAP);
    type_ = type;
    new (&data_.map) MapCow();
    data_.map = map;
  }

  Element::Element(const SequenceCow& sequence, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::SEQUENCE);
    type_ = type;
    new (&data_.seq) SequenceCow();
    data_.seq = sequence;
  }

  Element::Element(SequenceCow&& sequence, NodeType type) noexcept {
    assert(get_internal_data_type(type) == InternalDataType::SEQUENCE);
    type_ = type;
    new (&data_.seq) SequenceCow();
    data_.seq = sequence;
  }

  Element::Element(const Element& rhs) noexcept {
    init_data(rhs.type_);
    copy_data(rhs);
  }

  Element::Element(Element&& rhs) noexcept {
    init_data(rhs.type_);
    swap_data(rhs);
  }

  Element& Element::operator=(const Element& o) noexcept {
    if (this != &o) {
      if (get_internal_data_type(type_) != get_internal_data_type(o.type_)) {
        destroy_data();
        init_data(o.type_);
      }
      copy_data(o);
    }
    return *this;
  }

  Element& Element::operator=(Element&& o) noexcept {
    if (get_internal_data_type(type_) == get_internal_data_type(o.type_)) {
      swap_data(o);
    } else {
      Element tmp(type_);
      swap_data(tmp);

      destroy_data();
      init_data(o.type_);
      swap_data(o);

      o.destroy_data();
      o.init_data(tmp.type_);
      o.swap_data(tmp);
    }
    return *this;
  }

  Element::~Element() noexcept {
    destroy_data();
  }

  const Sequence* Element::as_sequence() const {
    return get_internal_data_type(type_) == InternalDataType::SEQUENCE
      ? data_.seq.get()
      : nullptr;
  }

  const Map* Element::as_map() const {
    return get_internal_data_type(type_) == InternalDataType::MAP
      ? data_.map.get()
      : nullptr;
  }

  Sequence* Element::as_sequence_mut() {
    return get_internal_data_type(type_) == InternalDataType::SEQUENCE
      ? data_.seq.get_mut()
      : nullptr;
  }

  Map* Element::as_map_mut() {
    return get_internal_data_type(type_) == InternalDataType::MAP
      ? data_.map.get_mut()
      : nullptr;
  }

  Element Element::get(const Literal& key) const {
    auto map = as_map();
    if (map == nullptr) {
      spdlog::debug("The element {} isn't a map", repr());
      return Element();
    }

    auto search = map->find(key);
    return search == map->end() ? Element() : search->second;
  }

  Element Element::get(size_t index) const {
    auto seq = as_sequence();
    if (seq == nullptr) {
      spdlog::debug("The element {} isn't a sequence", repr());
      return Element();
    }

    return index < seq->size() ? (*seq)[index] : Element();
  }

  bool Element::has(const Literal& key) const {
    auto map = as_map();
    return (map == nullptr) ? false : map->count(key) > 0;
  }

  bool Element::is_scalar() const {
    switch (type()) {
      case NodeType::STR: // Fallback
      case NodeType::BIN: // Fallback
      case NodeType::INT64: // Fallback
      case NodeType::DOUBLE: // Fallback
      case NodeType::BOOL: // Fallback
      case NodeType::OVERRIDE_STR:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_string() const {
    switch (type()) {
      case NodeType::STR: // Fallback
      case NodeType::OVERRIDE_STR:
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
      case NodeType::NONE: // Fallback
      case NodeType::OVERRIDE_NONE:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_undefined() const {
    return type() == NodeType::UNDEFINED;
  }

  bool Element::is_override() const {
    switch (type()) {
      case NodeType::OVERRIDE_MAP: // Fallback
      case NodeType::OVERRIDE_SEQUENCE: // Fallback
      case NodeType::OVERRIDE_NONE: // Fallback
      case NodeType::OVERRIDE_STR:
        return true;
      default:
        break;
    }
    return false;
  }

  Element Element::clone_without_virtual() const {
    switch (type()) {
      case NodeType::UNDEFINED: // Fallback
      case NodeType::DELETE:
        return Element();
      case NodeType::MAP: // Fallback
      case NodeType::OVERRIDE_MAP:
        return Element(data_.map);
      case NodeType::SEQUENCE: // Fallback
      case NodeType::OVERRIDE_SEQUENCE: // Fallback
      case NodeType::FORMAT: // Fallback
      case NodeType::SREF: // Fallback
      case NodeType::REF:
        return Element(data_.seq);
      case NodeType::NONE: // Fallback
      case NodeType::OVERRIDE_NONE:
        return Element(NodeType::NONE);
      case NodeType::STR: // Fallback
      case NodeType::OVERRIDE_STR: // Fallback
      case NodeType::BIN:
        return Element(data_.literal);
      case NodeType::INT64:
        return Element(data_.int64_value);
      case NodeType::DOUBLE:
        return Element(data_.double_value);
      case NodeType::BOOL:
        return Element(data_.bool_value);
    }
    assert(false);
  }

  std::string Element::repr() const {
    std::stringstream ss;

    ss << "Element(";
    ss << "type: " << to_string(type());

    switch (get_internal_data_type(type_)) {
      case InternalDataType::EMPTY:
        break;
      case InternalDataType::MAP: // Fallback
        ss << ", size: " << as_map()->size();
        break;
      case InternalDataType::SEQUENCE: // Fallback
        ss << ", size: " << as_sequence()->size();
        break;
      case InternalDataType::LITERAL:
        ss << ", literal: '" << data_.literal.str();
        break;
      case InternalDataType::INT64:
        ss << ", int64: '" << data_.int64_value;
        break;
      case InternalDataType::DOUBLE:
        ss << ", double: '" << data_.double_value;
        break;
      case InternalDataType::BOOL:
        ss << ", bool: '" << data_.bool_value;
        break;
    }

    ss << "')";

    return ss.str();
  }

  void Element::freeze() {
    switch (get_internal_data_type(type_)) {
      case InternalDataType::MAP:
        data_.map.freeze([](auto* map) {
          map->rehash(0);
          for (auto& it: *map) it.second.freeze();
        });
        break;
      case InternalDataType::SEQUENCE:
        data_.seq.freeze([](auto* seq) {
          seq->shrink_to_fit();
          for (size_t i = 0, l = seq->size(); i < l; ++i) {
            (*seq)[i].freeze();
          }
        });
        break;
      default:
        break;
    }
  }

  std::array<uint8_t, 32> Element::make_checksum() const {
    std::string fingerprint;
    jmutils::push_varint(fingerprint, 0); // fingerprint & checksum version
    add_fingerprint(fingerprint);

    std::array<uint8_t, 32> checksum;

    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, fingerprint.data(), fingerprint.size());
    SHA256_Final(checksum.data(), &sha256);

    return checksum;
  }

  void Element::add_fingerprint(std::string& output) const {
    output.push_back((uint8_t) type_);

    switch (get_internal_data_type(type_)) {
      case InternalDataType::MAP: {
        auto map = data_.map.get();
        std::vector<std::pair<std::string, Element>> sorted_values;
        sorted_values.reserve(map->size());
        for (const auto& it: *map) {
          sorted_values.emplace_back(it.first.str(), it.second);
        }
        std::sort(
          sorted_values.begin(),
          sorted_values.end(),
          [](const auto& a, const auto& b) { return a.first < b.first; }
        );

        jmutils::push_varint(output, map->size());
        for (size_t i = 0, l = sorted_values.size(); i < l; ++i) {
          jmutils::push_str(output, sorted_values[i].first);
          sorted_values[i].second.add_fingerprint(output);
        }
        break;
      }

      case InternalDataType::SEQUENCE: {
        auto seq = data_.seq.get();
        jmutils::push_varint(output, seq->size());
        for (const auto& x: *seq) {
          x.add_fingerprint(output);
        }
        break;
      }

      case InternalDataType::LITERAL:
        jmutils::push_str(output, data_.literal.str());
        break;

      case InternalDataType::INT64:
        jmutils::push_uint64(output, data_.uint64_value);
        break;

      case InternalDataType::DOUBLE:
        jmutils::push_double(output, data_.double_value);
        break;

      case InternalDataType::BOOL:
        output.push_back(data_.bool_value ? 0 : 1);
        break;

      case InternalDataType::EMPTY:
        break;
    }
  }

  void Element::init_data(NodeType type) noexcept {
    type_ = type;
    switch (get_internal_data_type(type_)) {
      case InternalDataType::MAP:
        new (&data_.map) MapCow();
        break;
      case InternalDataType::SEQUENCE:
        new (&data_.seq) SequenceCow();
        break;
      case InternalDataType::LITERAL:
        new (&data_.literal) Literal();
        break;
      default:
        break;
    }
  }

  void Element::destroy_data() noexcept {
    switch (get_internal_data_type(type_)) {
      case InternalDataType::MAP:
        data_.map.~MapCow();
        break;
      case InternalDataType::SEQUENCE:
        data_.seq.~SequenceCow();
        break;
      case InternalDataType::LITERAL:
        data_.literal.~Literal();
        break;
      default:
        break;
    }

    type_ = NodeType::UNDEFINED;
  }

  void Element::copy_data(const Element& o) noexcept {
    switch (get_internal_data_type(o.type_)) {
      case InternalDataType::MAP:
        data_.map = o.data_.map;
        break;
      case InternalDataType::SEQUENCE:
        data_.seq = o.data_.seq;
        break;
      case InternalDataType::LITERAL:
        data_.literal = o.data_.literal;
        break;
      case InternalDataType::INT64:
        data_.int64_value = o.data_.int64_value;
        break;
      case InternalDataType::DOUBLE:
        data_.double_value = o.data_.double_value;
        break;
      case InternalDataType::BOOL:
        data_.bool_value = o.data_.bool_value;
        break;
      case InternalDataType::EMPTY:
        break;
    }

    type_ = o.type_;
  }

  void Element::swap_data(Element& o) noexcept {
    switch (get_internal_data_type(o.type_)) {
      case InternalDataType::MAP:
        std::swap(data_.map, o.data_.map);
        break;
      case InternalDataType::SEQUENCE:
        std::swap(data_.seq, o.data_.seq);
        break;
      case InternalDataType::LITERAL:
        std::swap(data_.literal, o.data_.literal);
        break;
      case InternalDataType::INT64:
        std::swap(data_.int64_value, o.data_.int64_value);
        break;
      case InternalDataType::DOUBLE:
        std::swap(data_.double_value, o.data_.double_value);
        break;
      case InternalDataType::BOOL:
        std::swap(data_.bool_value, o.data_.bool_value);
        break;
      case InternalDataType::EMPTY:
        break;
    }

    std::swap(type_, o.type_);
  }

  namespace conversion
  {
    template <>
    std::pair<bool, jmutils::string::String> as<jmutils::string::String>(NodeType type, const data_t& data) {
      return get_internal_data_type(type) == InternalDataType::LITERAL
        ? std::make_pair(true, data.literal)
        : std::make_pair(false, jmutils::string::String());
    }

    template <>
    std::pair<bool, std::string> as<std::string>(NodeType type, const data_t& data) {
      switch (get_internal_data_type(type)) {
        case InternalDataType::LITERAL:
          return std::make_pair(true, data.literal.str());
        case InternalDataType::BOOL:
          return std::make_pair(true, data.bool_value ? "true" : "false");
        case InternalDataType::INT64:
          return std::make_pair(true, std::to_string(data.int64_value));
        case InternalDataType::DOUBLE:
          return std::make_pair(true, std::to_string(data.double_value));
        default:
          break;
      }
      return std::make_pair(false, "");
    }

    template <>
    std::pair<bool, bool> as<bool>(NodeType type, const data_t& data) {
      return get_internal_data_type(type) == InternalDataType::BOOL
        ? std::make_pair(true, data.bool_value)
        : std::make_pair(false, false);
    }

    template <>
    std::pair<bool, int64_t> as<int64_t>(NodeType type, const data_t& data) {
      return get_internal_data_type(type) == InternalDataType::INT64
        ? std::make_pair(true, data.int64_value)
        : std::make_pair(false, 0l);
    }

    template <>
    std::pair<bool, double> as<double>(NodeType type, const data_t& data) {
      return get_internal_data_type(type) == InternalDataType::DOUBLE
        ? std::make_pair(true, data.double_value)
        : std::make_pair(false, 0.0);
    }

  } /* conversion */
}
