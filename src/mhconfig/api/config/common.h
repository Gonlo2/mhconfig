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

enum ValueElement {
  STR_VALUE_ELEMENT = 0,
  UNDEFINED_VALUE_ELEMENT = 1,
  INT_VALUE_ELEMENT = 2,
  FLOAT_VALUE_ELEMENT = 3,
  BOOL_VALUE_ELEMENT = 4,
  NULL_VALUE_ELEMENT = 5,
  MAP_VALUE_ELEMENT = 6,
  SEQUENCE_VALUE_ELEMENT = 7
};

enum KeyElement {
  STR_KEY_ELEMENT = 0
};


template <typename T>
uint32_t fill_elements(
  const Element& root,
  T* container,
  proto::Element* output
) {
  switch (root.type()) {
    case NodeType::UNDEFINED_NODE: {
      output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      return 1;
    }

    case NodeType::NULL_NODE: // Fallback
    case NodeType::OVERRIDE_NULL_NODE: {
      output->set_type(output->type() | ValueElement::NULL_VALUE_ELEMENT);
      return 1;
    }

    case NodeType::STR_NODE: // Fallback
    case NodeType::OVERRIDE_STR_NODE: {
      auto r = root.try_as<std::string>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::STR_VALUE_ELEMENT);
        output->set_value_str(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::INT_NODE: // Fallback
    case NodeType::OVERRIDE_INT_NODE: {
      auto r = root.try_as<int64_t>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::INT_VALUE_ELEMENT);
        output->set_value_int(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::FLOAT_NODE: // Fallback
    case NodeType::OVERRIDE_FLOAT_NODE: {
      auto r = root.try_as<double>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::FLOAT_VALUE_ELEMENT);
        output->set_value_float(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::BOOL_NODE: // Fallback
    case NodeType::OVERRIDE_BOOL_NODE: {
      auto r = root.try_as<bool>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::BOOL_VALUE_ELEMENT);
        output->set_value_bool(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case ::mhconfig::MAP_NODE: // Fallback
    case ::mhconfig::OVERRIDE_MAP_NODE: {
      output->set_type(output->type() | ValueElement::MAP_VALUE_ELEMENT);
      auto map = root.as_map();
      output->set_size(map->size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto& it : *map) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(it.second, container, value);
        value->set_type(value->type() | (KeyElement::STR_KEY_ELEMENT<<4));
        value->set_key_str(it.first.str());
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case ::mhconfig::SEQUENCE_NODE: // Fallback
    case ::mhconfig::FORMAT_NODE: // Fallback
    case ::mhconfig::SREF_NODE: // Fallback
    case ::mhconfig::REF_NODE: // Fallback
    case ::mhconfig::OVERRIDE_SEQUENCE_NODE: {
      output->set_type(output->type() | ValueElement::SEQUENCE_VALUE_ELEMENT);
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
  }

  output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
  return 1;
}

} /* config */
} /* api */
} /* mhconfig */

#endif
