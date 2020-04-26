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
  const std::vector<std::string>& key,
  ::mhconfig::proto::GetResponse& msg
) {
  ElementRef element = element_;
  //for (const auto& x: key) {
    //element = element_->get(string_pool::String(x));
  //}

  //FIXME Ignore the first key
  for (uint32_t i = 1; i < key.size(); ++i) {
    element = element->get(string_pool::String(key[i]));
  }

  fill_elements(element, &msg, msg.add_elements());
}

} /* config */
} /* api */
} /* mhconfig */
