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
  mhconfig::Element* root,
  T* container,
  ::mhconfig::proto::Element* output
) {
  switch (root->type()) {
    case ::mhconfig::UNDEFINED_NODE: {
      output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      return 1;
    }

    case ::mhconfig::NULL_NODE: {
      output->set_type(output->type() | ValueElement::NULL_VALUE_ELEMENT);
      return 1;
    }

    case NodeType::SCALAR_NODE: // Fallback
    case NodeType::STR_NODE: {
      auto r = root->try_as<std::string>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::STR_VALUE_ELEMENT);
        output->set_value_str(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::INT_NODE: {
      auto r = root->try_as<int64_t>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::INT_VALUE_ELEMENT);
        output->set_value_int(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::FLOAT_NODE: {
      auto r = root->try_as<double>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::FLOAT_VALUE_ELEMENT);
        output->set_value_float(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case NodeType::BOOL_NODE: {
      auto r = root->try_as<bool>();
      if (r.first) {
        output->set_type(output->type() | ValueElement::BOOL_VALUE_ELEMENT);
        output->set_value_bool(r.second);
      } else {
        output->set_type(output->type() | ValueElement::UNDEFINED_VALUE_ELEMENT);
      }
      return 1;
    }

    case ::mhconfig::MAP_NODE: {
      output->set_type(output->type() | ValueElement::MAP_VALUE_ELEMENT);
      output->set_size(root->as_map().size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto& it : root->as_map()) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(it.second.get(), container, value);
        value->set_type(value->type() | (KeyElement::STR_KEY_ELEMENT<<4));
        value->set_key_str(it.first.str());
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case ::mhconfig::SEQUENCE_NODE: {
      output->set_type(output->type() | ValueElement::SEQUENCE_VALUE_ELEMENT);
      output->set_size(root->as_sequence().size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto x : root->as_sequence()) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(x.get(), container, value);
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
