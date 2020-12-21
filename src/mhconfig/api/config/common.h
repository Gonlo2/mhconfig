#ifndef MHCONFIG__API__CONFIG__COMMON_H
#define MHCONFIG__API__CONFIG__COMMON_H

#include <string>
#include <vector>

#include "mhconfig/element.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"

namespace mhconfig
{
namespace api
{
namespace config
{

enum class ValueElement {
  STR = 0,
  UNDEFINED = 1,
  INT64 = 2,
  DOUBLE = 3,
  BOOL = 4,
  NONE = 5,
  MAP = 6,
  SEQUENCE = 7,
  BIN = 8
};

enum class KeyElement {
  STR_KEY_ELEMENT = 0
};


namespace {
  template <typename T>
  inline void add_value_type(T* message, ValueElement element) {
    message->set_type(message->type() | static_cast<uint32_t>(element));
  }

  template <typename T>
  inline void add_key_type(T* message, KeyElement element) {
    message->set_type(message->type() | (static_cast<uint32_t>(element)<<4));
  }
}


template <typename T>
uint32_t fill_elements(
  const Element& root,
  T* container,
  proto::Element* output
) {
  switch (root.type()) {
    case NodeType::UNDEFINED: {
      add_value_type(output, ValueElement::UNDEFINED);
      return 1;
    }

    case NodeType::NONE: // Fallback
    case NodeType::OVERRIDE_NONE: {
      add_value_type(output, ValueElement::NONE);
      return 1;
    }

    case NodeType::STR: // Fallback
    case NodeType::OVERRIDE_STR: {
      if (auto r = root.try_as<std::string>(); r) {
        add_value_type(output, ValueElement::STR);
        output->set_value_str(*r);
      } else {
        add_value_type(output, ValueElement::UNDEFINED);
      }
      return 1;
    }

    case NodeType::BIN: {
      if (auto r = root.try_as<std::string>(); r) {
        add_value_type(output, ValueElement::BIN);
        output->set_value_bin(*r);
      } else {
        add_value_type(output, ValueElement::UNDEFINED);
      }
      return 1;
    }

    case NodeType::INT64: {
      if (auto r = root.try_as<int64_t>(); r) {
        add_value_type(output, ValueElement::INT64);
        output->set_value_int(*r);
      } else {
        add_value_type(output, ValueElement::UNDEFINED);
      }
      return 1;
    }

    case NodeType::DOUBLE: {
      if (auto r = root.try_as<double>(); r) {
        add_value_type(output, ValueElement::DOUBLE);
        output->set_value_double(*r);
      } else {
        add_value_type(output, ValueElement::UNDEFINED);
      }
      return 1;
    }

    case NodeType::BOOL: {
      if (auto r = root.try_as<bool>(); r) {
        add_value_type(output, ValueElement::BOOL);
        output->set_value_bool(*r);
      } else {
        add_value_type(output, ValueElement::UNDEFINED);
      }
      return 1;
    }

    case NodeType::MAP: // Fallback
    case NodeType::OVERRIDE_MAP: {
      add_value_type(output, ValueElement::MAP);
      auto map = root.as_map();
      output->set_size(map->size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto& it : *map) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(it.second, container, value);
        add_key_type(value, KeyElement::STR_KEY_ELEMENT);
        value->set_key_str(it.first.str());
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case NodeType::SEQUENCE: // Fallback
    case NodeType::OVERRIDE_SEQUENCE: {
      add_value_type(output, ValueElement::SEQUENCE);
      auto seq = root.as_sequence();
      output->set_size(seq->size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (size_t i = 0, l = seq->size(); i < l; ++i) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements((*seq)[i], container, value);
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case NodeType::DELETE: // Fallback
    case NodeType::FORMAT: // Fallback
    case NodeType::SREF: // Fallback
    case NodeType::REF:
      assert(false);
  }

  add_value_type(output, ValueElement::UNDEFINED);
  return 1;
}

} /* config */
} /* api */
} /* mhconfig */

#endif
