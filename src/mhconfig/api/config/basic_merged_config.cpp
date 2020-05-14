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
  //for (const auto& x: key) {
    //element = element_->get(::string_pool::String(x));
  //}

  //FIXME Ignore the first key
  for (uint32_t i = 1; i < api_request->key().size(); ++i) {
    element = element->get(::string_pool::String(api_request->key()[i]));
  }

  api_request->set_element(element);
}

} /* config */
} /* api */
} /* mhconfig */
