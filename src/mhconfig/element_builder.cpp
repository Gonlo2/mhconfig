#include "mhconfig/element_builder.h"

namespace mhconfig
{

ElementBuilder::ElementBuilder(
  logger::ReplayLogger& logger,
  jmutils::string::Pool* pool,
  const std::string& document,
  absl::flat_hash_set<std::string>& reference_to
) : logger_(logger),
  pool_(pool),
  document_(document),
  reference_to_(reference_to)
{
}

Element ElementBuilder::make_and_check(YAML::Node &node) {
  auto element = make(node);

  if (element.tag() == Element::Tag::REF) {
    if (!is_a_valid_path(element)) {
      return make_element(node);
    }
    auto ref_document = element.as_seq()->front().as<std::string>();
    if (document_ == ref_document) {
      return make_element_and_error(
        "A reference can't use the same document",
        node
      );
    } else {
      reference_to_.insert(ref_document);
    }
  } else if (element.tag() == Element::Tag::SREF) {
    if (!is_a_valid_path(element)) {
      return make_element(node);
    }
  }

  return element;
}

Element ElementBuilder::make(YAML::Node &node) {
  switch (node.Type()) {
    case YAML::NodeType::Null: {
      if ((node.Tag() == "") || (node.Tag() == TAG_NONE)) {
        return make_element_and_trace(
          "Created null value",
          node,
          Element::Type::NONE
        );
      } else if (node.Tag() == TAG_OVERRIDE) {
        auto element = make_element_and_trace(
          "Created null value with a override tag",
          node,
          Element::Type::NONE
        );
        element.set_tag(Element::Tag::OVERRIDE);
        return element;
      }
      return make_element_and_error("Unknown tag for a null value", node);
    }
    case YAML::NodeType::Scalar:
      return make_from_scalar(node);
    case YAML::NodeType::Sequence:
      return make_from_seq(node);
    case YAML::NodeType::Map:
      return make_from_map(node);
    case YAML::NodeType::Undefined:
      return make_element_and_error("Created undefined value", node);
  }

  return make_element_and_error("Unknown node type", node);
}

Element ElementBuilder::make_from_scalar(YAML::Node &node) {
  if ((node.Tag() == TAG_NON_PLAIN_SCALAR) || (node.Tag() == TAG_STR)) {
    return make_element_and_trace(
      "Created string value",
      node,
      pool_->add(node.as<std::string>())
    );
  } else if (node.Tag() == TAG_PLAIN_SCALAR) {
    return make_from_plain_scalar(node);
  } else if (node.Tag() == TAG_FORMAT) {
    return make_from_format(node);
  } else if (node.Tag() == TAG_BIN) {
    std::string encoded_value = node.as<std::string>();

    if (!jmutils::base64_sanitize(encoded_value)) {
      return make_element_and_error(
        "The base 64 value don't have a valid structure",
        node
      );
    }

    std::string binary_value;
    jmutils::base64_decode(encoded_value, binary_value);
    return make_element_and_trace(
      "Created binary value",
      node,
      pool_->add(binary_value),
      Element::Type::BIN
    );
  } else if (node.Tag() == TAG_INT) {
    Element result = make_from_int64(node);
    if (result.is_undefined()) {
      logger_.error("Can't parse as a int64", result);
    } else {
      logger_.trace("Created int64 value", result);
    }
    return result;
  } else if (node.Tag() == TAG_DOUBLE) {
    Element result = make_from_double(node);
    if (result.is_undefined()) {
      logger_.error("Can't parse as a double", result);
    } else {
      logger_.trace("Created double value", result);
    }
    return result;
  } else if (node.Tag() == TAG_BOOL) {
    Element result = make_from_bool(node);
    if (result.is_undefined()) {
      logger_.error("Can't parse as a bool", result);
    } else {
      logger_.trace("Created bool value", result);
    }
    return result;
  } else if (node.Tag() == TAG_DELETE) {
    Element element = make_element_and_trace(
      "Created value with a delete tag (the literal has been ignored)",
      node
    );
    element.set_tag(Element::Tag::DELETE);
    return element;
  } else if (node.Tag() == TAG_OVERRIDE) {
    Element element = make_element_and_trace(
      "Created string value with a override tag",
      node,
      pool_->add(node.as<std::string>())
    );
    element.set_tag(Element::Tag::OVERRIDE);
    return element;
  }

  return make_element_and_error("Unknown tag for a scalar value", node);
}

Element ElementBuilder::make_from_plain_scalar(YAML::Node &node) {
  Element e = make_from_bool(node);
  if (!e.is_undefined()) {
    logger_.trace("Created bool value from plain scalar", e);
    return e;
  }

  e = make_from_int64(node);
  if (!e.is_undefined()) {
    logger_.trace("Created int64 value from plain scalar", e);
    return e;
  }

  e = make_from_double(node);
  if (!e.is_undefined()) {
    logger_.trace("Created double value from plain scalar", e);
    return e;
  }

  return make_element_and_trace(
    "Created string value from plain scalar",
    node,
    pool_->add(node.as<std::string>())
  );
}

Element ElementBuilder::make_from_format(YAML::Node &node) {
  Element element = make_element(node);
  Seq tmpl_seq;

  auto tmpl = node.as<std::string>();
  for (size_t i = 0, l = tmpl.size(); i < l; ++i) {
    std::stringstream ss;
    for (;i < l; ++i) {
      if (tmpl[i] == '{') {
        if (++i >= l) {
          auto msg = fmt::format("The template has a unmatched '{{' at position {}", i);
          logger_.error(pool_->add(msg), element);
          return element;
        }
        if (tmpl[i] != '{') break;
      } else if (tmpl[i] == '}') {
        if ((++i >= l) || (tmpl[i] != '}')) {
          auto msg = fmt::format("The template has a unmatched '}}' at position '{}'", i);
          logger_.error(pool_->add(msg), element);
          return element;
        }
      }
      ss << tmpl[i];
    }

    if (auto x = ss.str(); !x.empty()) {
      tmpl_seq.push_back(make_element(node, pool_->add(x)));
    }

    if (i < l) {
      Seq arg_seq;
      Element::Tag tag = Element::Tag::NONE;
      if (auto slice = parse_format_slice(element, tmpl, i)) {
        if (document_ == *slice) {
          tag = Element::Tag::SREF;
        } else {
          tag = Element::Tag::REF;
          arg_seq.push_back(
            make_element(node, pool_->add(*slice))
          );
          reference_to_.insert(*slice);
        }
      } else {
        return element;
      }

      while ((i < l) && (tmpl[i] != '}')) {
        if (auto slice = parse_format_slice(element, tmpl, ++i)) {
          arg_seq.push_back(
            make_element(node, pool_->add(*slice))
          );
        } else {
          return element;
        }
      }

      Element e(std::move(arg_seq));
      e.set_tag(tag);
      tmpl_seq.push_back(std::move(e.set_origin(element)));
    }
  }

  element = make_element_and_trace(
    "Created template",
    node,
    std::move(tmpl_seq)
  );
  element.set_tag(Element::Tag::FORMAT);
  return element;
}

Element ElementBuilder::make_from_int64(YAML::Node &node) {
  auto str(node.as<std::string>());
  char *end;
  errno = 0;
  int64_t value = std::strtoll(str.c_str(), &end, 10);
  return errno || (str.c_str() == end) || (*end != 0)
    ? make_element(node)
    : make_element(node, value);
}

Element ElementBuilder::make_from_double(YAML::Node &node) {
  auto str(node.as<std::string>());
  char *end;
  errno = 0;
  double value = std::strtod(str.c_str(), &end);
  return errno || (str.c_str() == end) || (*end != 0)
    ? make_element(node)
    : make_element(node, value);
}

Element ElementBuilder::make_from_bool(YAML::Node &node) {
  auto str(node.as<std::string>());
  if (str == "true") {
    return make_element(node, true);
  } else if (str == "false") {
    return make_element(node, false);
  }
  return make_element(node);
}

Element ElementBuilder::make_from_map(YAML::Node &node) {
  Map map;
  map.reserve(node.size());
  for (auto it : node) {
    if (!it.first.IsScalar()) {
      return make_element_and_error("The key of a map must be a scalar", node);
    }
    jmutils::string::String key(pool_->add(it.first.as<std::string>()));
    map[key] = make_and_check(it.second);
  }

  if (node.Tag() == TAG_PLAIN_SCALAR) {
    return make_element_and_trace("Created map value", node, std::move(map));
  } else if (node.Tag() == TAG_OVERRIDE) {
    Element element = make_element_and_trace(
      "Created map value with a override tag",
      node,
      std::move(map)
    );
    element.set_tag(Element::Tag::OVERRIDE);
    return element;
  }

  return make_element_and_error("Unknown tag for a map value", node);
}

Element ElementBuilder::make_from_seq(YAML::Node &node) {
  Seq seq;
  seq.reserve(node.size());
  for (auto it : node) {
    seq.push_back(make_and_check(it));
  }

  if (node.Tag() == TAG_PLAIN_SCALAR) {
    return make_element_and_trace(
      "Created sequence value",
      node,
      std::move(seq)
    );
  } else if (node.Tag() == TAG_SREF) {
    Element element = make_element_and_trace(
      "Created sref value",
      node,
      std::move(seq)
    );
    element.set_tag(Element::Tag::SREF);
    return element;
  } else if (node.Tag() == TAG_REF) {
    Element element = make_element_and_trace(
      "Created ref value",
      node,
      std::move(seq)
    );
    element.set_tag(Element::Tag::REF);
    return element;
  } else if (node.Tag() == TAG_OVERRIDE) {
    Element element = make_element_and_trace(
      "Created sequence value with a override tag",
      node,
      std::move(seq)
    );
    element.set_tag(Element::Tag::OVERRIDE);
    return element;
  }

  return make_element_and_error("Unknown tag for a sequence value", node);
}

bool ElementBuilder::is_a_valid_path(
  const Element& element
) {
  const auto path = element.as_seq();

  if (path->empty()) {
    logger_.error(
      "A reference must be a sequence with at least one element",
      element
    );
    return false;
  }

  for (size_t i = 0, l = path->size(); i < l; ++i) {
    if (!(*path)[i].is_string()) {
      logger_.error(
        "All the elements of a reference must be strings",
        element
      );
      return false;
    }
  }

  return true;
}

std::optional<std::string> ElementBuilder::parse_format_slice(
  const Element& element,
  const std::string& tmpl,
  size_t& idx
) {
  size_t start = idx;
  for (size_t l = tmpl.size(); idx < l; ++idx) {
    if ((tmpl[idx] == '}') || (tmpl[idx] == '/')) {
      return std::optional<std::string>(std::string(&tmpl[start], idx-start));
    } else if (tmpl[idx] == '{') {
      break;
    }
  }
  auto msg = fmt::format("The template has a unmatched '{{' at position {}", start);
  logger_.error(pool_->add(msg), element);
  return std::optional<std::string>();
}

} /* mhconfig */
