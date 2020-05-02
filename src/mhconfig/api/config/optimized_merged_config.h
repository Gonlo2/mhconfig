#ifndef MHCONFIG__API__CONFIG__OPTIMIZED_MERGED_CONFIG_H
#define MHCONFIG__API__CONFIG__OPTIMIZED_MERGED_CONFIG_H

#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/api/config/common.h"

#include "spdlog/spdlog.h"
#include "string_pool/pool.h"
#include "jmutils/common.h"

#include <cmph.h>


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
  std::vector<std::pair<std::string, std::pair<uint32_t, uint32_t>>>& output
);

void make_elements_ranges_map(
  mhconfig::ElementRef root,
  std::vector<std::pair<std::string, std::pair<uint32_t, uint32_t>>>& output
);

struct key_position_t {
  string_pool::String key;
  uint32_t start;
  uint32_t size;
};

class OptimizedMergedConfig : public MergedConfig
{
public:
  OptimizedMergedConfig();
  virtual ~OptimizedMergedConfig();

  OptimizedMergedConfig(const OptimizedMergedConfig&) = delete;
  OptimizedMergedConfig(OptimizedMergedConfig&&) = delete;

  bool init(
    ElementRef element,
    std::shared_ptr<string_pool::Pool> pool
  );

  void add_elements(
    request::GetRequest* api_request
  ) override;

private:
  cmph_t* hash_{nullptr};
  std::vector<key_position_t> position_;
  std::string data_;
};

} /* config */
} /* api */
} /* mhconfig */

#endif
