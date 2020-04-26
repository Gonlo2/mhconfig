#ifndef MHCONFIG__API__CONFIG__BASIC_MERGED_CONFIG_H
#define MHCONFIG__API__CONFIG__BASIC_MERGED_CONFIG_H

#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/element.h"

namespace mhconfig
{
namespace api
{
namespace config
{

class BasicMergedConfig : public MergedConfig
{
public:
  BasicMergedConfig(ElementRef element);
  virtual ~BasicMergedConfig();

  void add_elements(
    const std::vector<std::string>& key,
    ::mhconfig::proto::GetResponse& msg
  ) override;

private:
  ElementRef element_{nullptr};
};

} /* config */
} /* api */
} /* mhconfig */

#endif
