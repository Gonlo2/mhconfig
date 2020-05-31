#include "mhconfig/element.h"

namespace mhconfig {
  std::string to_string(NodeType type) {
    switch (type) {
      case UNDEFINED_NODE: return "UNDEFINED";
      case MAP_NODE: return "MAP";
      case SEQUENCE_NODE: return "SEQUENCE";
      case NULL_NODE: return "NULL";
      case SCALAR_NODE: return "SCALAR";
      case STR_NODE: return "STRING";
      case INT_NODE: return "INTEGER";
      case FLOAT_NODE: return "FLOAT";
      case BOOL_NODE: return "BOOLEAN";
    }

    return "unknown";
  }

  NodeType scalar_type(const Literal& tag) {
    if (tag == TAG_STR) {
      return STR_NODE;
    } else if (tag == TAG_INT) {
      return INT_NODE;
    } else if (tag == TAG_FLOAT) {
      return FLOAT_NODE;
    } else if (tag == TAG_BOOL) {
      return BOOL_NODE;
    }

    return SCALAR_NODE;
  }


  Element::Element() :
    type_(UNDEFINED_NODE),
    tag_(TAG_EMPTY)
  {
  }

  Element::Element(NodeType type) :
    type_(type),
    tag_(TAG_EMPTY)
  {}

  Element::Element(const Literal& literal) :
    type_(SCALAR_NODE),
    tag_(TAG_EMPTY),
    literal_(literal)
  {}

  Element::Element(const Literal& literal, const Literal& tag) :
    type_(scalar_type(tag)),
    tag_(tag),
    literal_(literal)
  {}

  Element::Element(const MapRef map) :
    type_(MAP_NODE),
    tag_(TAG_EMPTY),
    map_(map)
  {}

  Element::Element(const MapRef map, const Literal& tag) :
    type_(MAP_NODE),
    tag_(tag),
    map_(map)
  {}

  Element::Element(const SequenceRef sequence) :
    type_(SEQUENCE_NODE),
    tag_(TAG_EMPTY),
    sequence_(sequence)
  {}

  Element::Element(const SequenceRef sequence, const Literal& tag) :
    type_(SEQUENCE_NODE),
    tag_(tag),
    sequence_(sequence)
  {}

  const Sequence& Element::as_sequence() const {
    assert(type_ == SEQUENCE_NODE);
    return *sequence_;
  }

  const Map& Element::as_map() const {
    assert(type_ == MAP_NODE);
    return *map_;
  }

  ElementRef Element::get(const Literal& key) const {
    if (type_ != MAP_NODE) {
      spdlog::get("console")->debug("The element {} isn't a map", repr());
      return UNDEFINED_ELEMENT;
    }

    auto search = map_->find(key);
    return (search == map_->end()) ? UNDEFINED_ELEMENT : search->second;
  }

  ElementRef Element::get(size_t index) const {
    if (type_ != SEQUENCE_NODE) {
      spdlog::get("console")->debug("The element {} isn't a sequence", repr());
      return UNDEFINED_ELEMENT;
    }

    return (sequence_->size() <= index) ? UNDEFINED_ELEMENT : (*sequence_)[index];
  }

  bool Element::has(const Literal& key) const {
    return (type_ == MAP_NODE)
      ? map_->count(key) > 0
      : false;
  }

  const Literal& Element::tag() const {
    return tag_;
  }

  bool Element::is_scalar() const {
    switch (type_) {
      case NodeType::SCALAR_NODE: // Fallback
      case NodeType::STR_NODE: // Fallback
      case NodeType::INT_NODE: // Fallback
      case NodeType::FLOAT_NODE: // Fallback
      case NodeType::BOOL_NODE:
        return true;
      default:
        return false;
    }
  }
  bool Element::is_map() const {
    return type_ == MAP_NODE;
  }
  bool Element::is_sequence() const {
    return type_ == SEQUENCE_NODE;
  }
  bool Element::is_null() const {
    return type_ == NULL_NODE;
  }
  bool Element::is_undefined() const {
    return type_ == UNDEFINED_NODE;
  }

  std::pair<bool, std::string> Element::to_yaml() const {
    YAML::Emitter out;
    out.SetIndent(2);

    out << YAML::BeginDoc;
    bool ok = to_yaml_base(out);

    return std::make_pair(
      ok && out.good(),
      std::string(out.c_str())
    );
  }

  bool Element::to_yaml_base(YAML::Emitter& out) const {
    if (tag_.size() > 1) {
      out << YAML::LocalTag(tag_.str().substr(1, tag_.size()-1));
    }

    switch (type_) {
      case MAP_NODE: {
        out << YAML::BeginMap;
        for (const auto& it : *map_) {
          out << YAML::Key << it.first.str();
          out << YAML::Value;
          if (!it.second->to_yaml_base(out)) return false;
        }
        out << YAML::EndMap;
        return out.good();
      }
      case SEQUENCE_NODE: {
        out << YAML::BeginSeq;
        for (const auto& e : *sequence_) {
          if (!e->to_yaml_base(out)) return false;
        }
        out << YAML::EndSeq;
        return out.good();
      }
      case NULL_NODE: {
        out << YAML::Flow;
        out << YAML::Null;
        out << YAML::Block;
        return out.good();
      }
      case SCALAR_NODE: // Fallback
      case STR_NODE: // Fallback
      case INT_NODE: // Fallback
      case FLOAT_NODE: // Fallback
      case BOOL_NODE: {
        out << YAML::Flow;
        out << literal_.str();
        out << YAML::Block;
        return out.good();
      }
      case UNDEFINED_NODE:
        return false;
    }
    return false;
  }

  ElementRef Element::clone_without_tag() const {
    switch (type_) {
      case MAP_NODE:
        return std::make_shared<Element>(map_);
      case SEQUENCE_NODE:
        return std::make_shared<Element>(sequence_);
      case SCALAR_NODE: // Fallback
      case STR_NODE: // Fallback
      case INT_NODE: // Fallback
      case FLOAT_NODE: // Fallback
      case BOOL_NODE:
        return std::make_shared<Element>(literal_);
    }

    return std::make_shared<Element>(type_);
  }

  std::string Element::repr() const {
    std::stringstream ss;

    ss << "Element(";
    ss << "type: " << to_string(type_) << ", ";

    switch (type_) {
      case NULL_NODE: break;
      case UNDEFINED_NODE: break;

      case MAP_NODE:
        ss << "size: " << map_->size() << ", ";
        break;

      case SEQUENCE_NODE:
        ss << "size: " << sequence_->size() << ", ";
        break;

      case SCALAR_NODE:
        ss << "value: '" << literal_.str() << "', ";
        break;
      case STR_NODE:
        ss << "str: '" << literal_.str() << "', ";
        break;
      case INT_NODE:
        ss << "int: '" << literal_.str() << "', ";
        break;
      case FLOAT_NODE:
        ss << "float: '" << literal_.str() << "', ";
        break;
      case BOOL_NODE:
        ss << "bool: '" << literal_.str() << "', ";
        break;
    }

    ss << "tag: '" << tag_.str() << "')";

    return ss.str();
  }

  NodeType Element::type() const {
    return type_;
  }

  namespace conversion
  {
    template <>
    std::pair<bool, ::string_pool::String> as<::string_pool::String>(NodeType type, const Literal& literal) {
      switch (type) {
        case NodeType::SCALAR_NODE: // Fallback
        case NodeType::STR_NODE: // Fallback
        case NodeType::INT_NODE: // Fallback
        case NodeType::FLOAT_NODE: // Fallback
        case NodeType::BOOL_NODE:
          return std::make_pair(true, literal);
      }
      return std::make_pair(false, literal);
    }

    template <>
    std::pair<bool, std::string> as<std::string>(NodeType type, const Literal& literal) {
      switch (type) {
        case NodeType::SCALAR_NODE: // Fallback
        case NodeType::STR_NODE: // Fallback
        case NodeType::INT_NODE: // Fallback
        case NodeType::FLOAT_NODE: // Fallback
        case NodeType::BOOL_NODE:
          return std::make_pair(true, literal.str());
      }
      return std::make_pair(false, "");
    }

    template <>
    std::pair<bool, bool> as<bool>(NodeType type, const Literal& literal) {
      if (type == NodeType::BOOL_NODE) {
        if (literal == "true") {
          return std::make_pair(true, true);
        } else if (literal == "false") {
          return std::make_pair(true, false);
        }
      }
      return std::make_pair(false, false);
    }

    template <>
    std::pair<bool, int64_t> as<int64_t>(NodeType type, const Literal& literal) {
      if (type == NodeType::INT_NODE) {
        auto str{literal.str()};
        int64_t value = std::strtoll(str.c_str(), nullptr, 10);
        return std::make_pair(errno == 0, value);
      }
      return std::make_pair(false, 0);
    }

    template <>
    std::pair<bool, double> as<double>(NodeType type, const Literal& literal) {
      if (type == NodeType::FLOAT_NODE) {
        auto str{literal.str()};
        double value = std::strtod(str.c_str(), nullptr);
        return std::make_pair(errno == 0, value);
      }
      return std::make_pair(false, 0);
    }

  } /* conversion */
}
