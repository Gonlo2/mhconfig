#include "mhconfig/element.h"

namespace mhconfig {
  std::string to_string(Element::Type type) {
    switch (type) {
      case Element::Type::UNDEFINED:
        return "UNDEFINED";
      case Element::Type::MAP:
        return "MAP";
      case Element::Type::SEQUENCE:
        return "SEQUENCE";
      case Element::Type::NONE:
        return "NONE";
      case Element::Type::STR:
        return "STR";
      case Element::Type::BIN:
        return "BIN";
      case Element::Type::INT64:
        return "INT64";
      case Element::Type::DOUBLE:
        return "DOUBLE";
      case Element::Type::BOOL:
        return "BOOLEAN";
    }

    return "unknown";
  }

  Element::Element() noexcept {
    init(Type::UNDEFINED);
  }

  Element::Element(Type type) noexcept {
    init(type);
  }

  Element::Element(int64_t value) noexcept {
    init(Type::INT64);
    data_.int64_value = value;
  }

  Element::Element(double value) noexcept {
    init(Type::DOUBLE);
    data_.double_value = value;
  }

  Element::Element(bool value) noexcept {
    init(Type::BOOL);
    data_.bool_value = value;
  }

  Element::Element(const Literal& value, Type type) noexcept {
    assert((type == Type::BIN) || (type == Type::STR));
    init(type);
    data_.literal = value;
  }

  Element::Element(Literal&& value, Type type) noexcept {
    assert((type == Type::BIN) || (type == Type::STR));
    init(type);
    data_.literal = std::move(value);
  }

  Element::Element(const Map& map) noexcept {
    init(Type::MAP);
    assert(data_.map.set(map));
  }

  Element::Element(Map&& map) noexcept {
    init(Type::MAP);
    assert(data_.map.set(std::move(map)));
  }

  Element::Element(const Seq& seq) noexcept {
    init(Type::SEQUENCE);
    assert(data_.seq.set(seq));
  }

  Element::Element(Seq&& seq) noexcept {
    init(Type::SEQUENCE);
    assert(data_.seq.set(std::move(seq)));
  }

  Element::Element(const Element& rhs) noexcept {
    init(rhs.type_);
    copy(rhs);
  }

  Element::Element(Element&& rhs) noexcept {
    init(rhs.type_);
    swap(rhs);
  }

  Element& Element::operator=(const Element& o) noexcept {
    if (this != &o) {
      if (type_ != o.type_) {
        destroy_data();
        init_data(o.type_);
      }
      copy(o);
    }
    return *this;
  }

  Element& Element::operator=(Element&& o) noexcept {
    if (type_ != o.type_) {
      destroy_data();
      init_data(o.type_);
    }
    swap(o);
    return *this;
  }

  Element::~Element() noexcept {
    destroy_data();
  }

  const Seq* Element::as_seq() const {
    return type_ == Type::SEQUENCE ? data_.seq.get() : nullptr;
  }

  const Map* Element::as_map() const {
    return type_ == Type::MAP ? data_.map.get() : nullptr;
  }

  Seq* Element::as_seq_mut() {
    return type_ == Type::SEQUENCE ? data_.seq.get_mut() : nullptr;
  }

  Map* Element::as_map_mut() {
    return type_ == Type::MAP ? data_.map.get_mut() : nullptr;
  }

  Element Element::get(const std::string& key) const {
    jmutils::string::InternalString internal_string;
    auto k = jmutils::string::make_string(key, &internal_string);
    return get(k);
  }

  Element Element::get(const Literal& key) const {
    auto map = as_map();
    if (map == nullptr) {
      spdlog::debug("The element {} isn't a map", *this);
      return Element();
    }

    auto search = map->find(key);
    return search == map->end() ? Element() : search->second;
  }

  Element Element::get(size_t index) const {
    auto seq = as_seq();
    if (seq == nullptr) {
      spdlog::debug("The element {} isn't a sequence", *this);
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
      case Type::STR: // Fallback
      case Type::BIN: // Fallback
      case Type::INT64: // Fallback
      case Type::DOUBLE: // Fallback
      case Type::BOOL:
        return true;
      default:
        break;
    }
    return false;
  }

  bool Element::is_string() const {
    return type() == Type::STR;
  }

  bool Element::is_map() const {
    return as_map() != nullptr;
  }

  bool Element::is_seq() const {
    return as_seq() != nullptr;
  }

  bool Element::is_null() const {
    return type() == Type::NONE;
  }

  bool Element::is_undefined() const {
    return type() == Type::UNDEFINED;
  }

  bool Element::is_override() const {
    return tag_ == Tag::OVERRIDE;
  }

  void Element::freeze() {
    switch (type_) {
      case Type::MAP:
        data_.map.freeze([](auto* map) {
          map->rehash(0);
          for (auto& it: *map) it.second.freeze();
        });
        break;
      case Type::SEQUENCE:
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
    jmutils::push_varint(fingerprint, 1); // fingerprint & checksum version
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
    output.push_back((uint8_t) tag_);

    switch (type_) {
      case Type::MAP: {
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

      case Type::SEQUENCE: {
        auto seq = data_.seq.get();
        jmutils::push_varint(output, seq->size());
        for (const auto& x: *seq) {
          x.add_fingerprint(output);
        }
        break;
      }

      case Type::BIN: // Fallback
      case Type::STR:
        jmutils::push_str(output, data_.literal.str());
        break;

      case Type::INT64:
        jmutils::push_uint64(output, data_.uint64_value);
        break;

      case Type::DOUBLE:
        jmutils::push_double(output, data_.double_value);
        break;

      case Type::BOOL:
        output.push_back(data_.bool_value ? 0 : 1);
        break;
    }
  }

  std::optional<std::string> Element::to_yaml() const {
    YAML::Emitter out;
    out.SetIndent(2);

    out << YAML::BeginDoc;
    bool ok = to_yaml_base(out);
    out << YAML::EndDoc;

    return ok && out.good()
      ? std::optional<std::string>(out.c_str())
      : std::optional<std::string>();
  }

  bool Element::to_yaml_base(YAML::Emitter& out) const {
    switch (tag()) {
      case Tag::NONE:
        break;
      case Tag::FORMAT:
        out << YAML::LocalTag("format");
        break;
      case Tag::SREF:
        out << YAML::LocalTag("sref");
        break;
      case Tag::REF:
        out << YAML::LocalTag("ref");
        break;
      case Tag::DELETE:
        out << YAML::LocalTag("delete");
        break;
      case Tag::OVERRIDE:
        out << YAML::LocalTag("override");
        break;
      default:
        out << YAML::LocalTag("unknown"); //TODO
        break;
    }

    switch (type()) {
      case Type::MAP: {
        out << YAML::BeginMap;
        for (const auto& it : *as_map()) {
          out << YAML::Key << it.first.str();
          out << YAML::Value;
          if (!it.second.to_yaml_base(out)) return false;
        }
        out << YAML::EndMap;
        return true;
      }
      case Type::SEQUENCE: {
        out << YAML::BeginSeq;
        for (const auto& e : *as_seq()) {
          if (!e.to_yaml_base(out)) return false;
        }
        out << YAML::EndSeq;
        return true;
      }
      case Type::NONE: {
        out << YAML::Null;
        return true;
      }
      case Type::STR: {
        out << YAML::DoubleQuoted << data_.literal.str();
        return true;
      }
      case Type::BIN: {
        auto x = data_.literal.str();
        out << YAML::Binary((const unsigned char*)x.data(), x.size());
        return true;
      }
      case Type::INT64: {
        out << data_.int64_value;
        return true;
      }
      case Type::DOUBLE: {
        out << data_.double_value;
        return true;
      }
      case Type::BOOL: {
        out << data_.bool_value;
        return true;
      }
      case Type::UNDEFINED: {
        if (tag() == Tag::NONE) {
          out << YAML::LocalTag("undefined");
        }
        out << YAML::Null;
        return true;
      }
    }
    return false;
  }

  void Element::init(Type type) noexcept {
    tag_ = Tag::NONE;
    col_ = 0;
    line_ = 0;
    document_id_ = 0xffff;
    raw_config_id_ = 0xffff;

    init_data(type);
  }

  void Element::init_data(Type type) noexcept {
    type_ = type;
    switch (type_) {
      case Type::MAP:
        new (&data_.map) MapData();
        break;
      case Type::SEQUENCE:
        new (&data_.seq) SeqData();
        break;
      case Type::BIN: // Fallback
      case Type::STR:
        new (&data_.literal) Literal();
        break;
      default:
        break;
    }
  }

  void Element::destroy_data() noexcept {
    switch (type_) {
      case Type::MAP:
        data_.map.~MapData();
        break;
      case Type::SEQUENCE:
        data_.seq.~SeqData();
        break;
      case Type::BIN: // Fallback
      case Type::STR:
        data_.literal.~Literal();
        break;
      default:
        break;
    }

    type_ = Type::UNDEFINED;
  }

  void Element::copy(const Element& o) noexcept {
    switch (o.type_) {
      case Type::MAP:
        data_.map = o.data_.map;
        break;
      case Type::SEQUENCE:
        data_.seq = o.data_.seq;
        break;
      case Type::BIN: // Fallback
      case Type::STR:
        data_.literal = o.data_.literal;
        break;
      case Type::INT64:
        data_.int64_value = o.data_.int64_value;
        break;
      case Type::DOUBLE:
        data_.double_value = o.data_.double_value;
        break;
      case Type::BOOL:
        data_.bool_value = o.data_.bool_value;
        break;
    }

    type_ = o.type_;
    tag_ = o.tag_;
    col_ = o.col_;
    line_ = o.line_;
    document_id_ = o.document_id_;
    raw_config_id_ = o.raw_config_id_;
  }

  void Element::swap(Element& o) noexcept {
    switch (o.type_) {
      case Type::MAP:
        std::swap(data_.map, o.data_.map);
        break;
      case Type::SEQUENCE:
        std::swap(data_.seq, o.data_.seq);
        break;
      case Type::BIN: // Fallback
      case Type::STR:
        std::swap(data_.literal, o.data_.literal);
        break;
      case Type::INT64:
        std::swap(data_.int64_value, o.data_.int64_value);
        break;
      case Type::DOUBLE:
        std::swap(data_.double_value, o.data_.double_value);
        break;
      case Type::BOOL:
        std::swap(data_.bool_value, o.data_.bool_value);
        break;
    }

    bitf_swap(type_, o.type_);
    bitf_swap(tag_, o.tag_);
    std::swap(col_, o.col_);
    std::swap(line_, o.line_);
    std::swap(document_id_, o.document_id_);
    std::swap(raw_config_id_, o.raw_config_id_);
  }

  namespace conversion
  {
    template <>
    std::optional<jmutils::string::String> as<jmutils::string::String>(const Element& e) {
      return (e.type() == Element::Type::STR) || (e.type() == Element::Type::BIN)
        ? std::optional<jmutils::string::String>(e.data_.literal)
        : std::optional<jmutils::string::String>();
    }

    template <>
    std::optional<std::string> as<std::string>(const Element& e) {
      switch (e.type()) {
        case Element::Type::BIN: // Fallback
        case Element::Type::STR:
          return std::optional<std::string>(e.data_.literal.str());
        case Element::Type::BOOL:
          return std::optional<std::string>(e.data_.bool_value ? "true" : "false");
        case Element::Type::INT64:
          return std::optional<std::string>(std::to_string(e.data_.int64_value));
        case Element::Type::DOUBLE:
          return std::optional<std::string>(std::to_string(e.data_.double_value));
        default:
          break;
      }
      return std::optional<std::string>();
    }

    template <>
    std::optional<bool> as<bool>(const Element& e) {
      return e.type() == Element::Type::BOOL
        ? std::optional<bool>(e.data_.bool_value)
        : std::optional<bool>();
    }

    template <>
    std::optional<int64_t> as<int64_t>(const Element& e) {
      return e.type() == Element::Type::INT64
        ? std::optional<int64_t>(e.data_.int64_value)
        : std::optional<int64_t>();
    }

    template <>
    std::optional<double> as<double>(const Element& e) {
      return e.type() == Element::Type::DOUBLE
        ? std::optional<double>(e.data_.double_value)
        : std::optional<double>();
    }

  } /* conversion */
}
