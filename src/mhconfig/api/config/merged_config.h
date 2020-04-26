#ifndef MHCONFIG__API__CONFIG__MERGED_CONFIG_H
#define MHCONFIG__API__CONFIG__MERGED_CONFIG_H

#include <string>
#include <vector>

#include "mhconfig/api/request/request.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace api
{
namespace config
{

template <typename T>
uint32_t fill_elements(
  mhconfig::ElementRef root,
  T* container,
  ::mhconfig::proto::Element* output
) {
  switch (root->type()) {
    case ::mhconfig::UNDEFINED_NODE: {
      output->set_type(::mhconfig::proto::Element::UNDEFINED_NODE);
      return 1;
    }

    case ::mhconfig::NULL_NODE: {
      output->set_type(::mhconfig::proto::Element::NULL_NODE);
      return 1;
    }

    case ::mhconfig::SCALAR_NODE: {
      output->set_type(::mhconfig::proto::Element::SCALAR_NODE);
      output->set_value(root->as<std::string>());
      return 1;
    }

    case ::mhconfig::MAP_NODE: {
      output->set_type(::mhconfig::proto::Element::MAP_NODE);
      output->set_size(root->as_map().size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto& it : root->as_map()) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(it.second, container, value);
        value->set_key(it.first.str());
        value->set_sibling_offset(sibling_offset);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case ::mhconfig::SEQUENCE_NODE: {
      output->set_type(::mhconfig::proto::Element::SEQUENCE_NODE);
      output->set_size(root->as_sequence().size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto x : root->as_sequence()) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(x, container, value);
        value->set_sibling_offset(sibling_offset);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }
  }

  output->set_type(::mhconfig::proto::Element::UNDEFINED_NODE);
  return 1;
}

class MergedConfig
{
public:
  MergedConfig() {};
  virtual ~MergedConfig() {};

  virtual void add_elements(
    const std::vector<std::string>& key,
    ::mhconfig::proto::GetResponse& msg
  ) = 0;
};

} /* config */
} /* api */
} /* mhconfig */

#endif
