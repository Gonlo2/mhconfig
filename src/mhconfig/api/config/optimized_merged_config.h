#ifndef MHCONFIG__API__CONFIG__OPTIMIZED_MERGED_CONFIG_H
#define MHCONFIG__API__CONFIG__OPTIMIZED_MERGED_CONFIG_H

#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/api/config/common.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace config
{

uint32_t make_elements_ranges_map_rec(
  mhconfig::ElementRef root,
  uint32_t& idx,
  const std::string& skey,
  bool add,
  std::unordered_map<std::string, std::pair<uint32_t, uint32_t>>& output
);

void make_elements_ranges_map(
  mhconfig::ElementRef root,
  std::unordered_map<std::string, std::pair<uint32_t, uint32_t>>& output
);

class OptimizedMergedConfig : public MergedConfig
{
public:
  OptimizedMergedConfig();
  virtual ~OptimizedMergedConfig();

  bool init(ElementRef element);

  void add_elements(
    request::GetRequest* api_request
  ) override;

private:
  std::string data_;
  std::unordered_map<std::string, std::pair<uint32_t, uint32_t>> data_range_by_skey_;
};

} /* config */
} /* api */
} /* mhconfig */

#endif
