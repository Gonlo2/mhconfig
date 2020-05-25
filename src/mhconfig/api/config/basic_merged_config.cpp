#include "mhconfig/api/config/basic_merged_config.h"

namespace mhconfig
{
namespace api
{
namespace config
{

BasicMergedConfig::BasicMergedConfig(ElementRef element)
  : element_(element)
{
}

BasicMergedConfig::~BasicMergedConfig() {
}

void BasicMergedConfig::add_elements(
  request::GetRequest* api_request
) {
  ElementRef element = element_;
  ::string_pool::InternalString internal_string;
  for (const auto& s: api_request->key()) {
    element = element->get(::string_pool::make_string(s, &internal_string));
  }

  api_request->set_element(element);
}

} /* config */
} /* api */
} /* mhconfig */
