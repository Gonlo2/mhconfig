#ifndef MHCONFIG__DS__CONFIG_NAMESPACE_H
#define MHCONFIG__DS__CONFIG_NAMESPACE_H

#include <vector>
#include <map>
#include <unordered_map>
//#include <unordered_set>
#include <string>
#include <memory>

#include "string_pool/pool.h"

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/element.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace ds
{
namespace config_namespace
{

const static uint8_t NUMBER_OF_GC_GENERATIONS{3};

struct raw_config_t {
  uint32_t id{0};
  ElementRef value{nullptr};
  std::unordered_set<std::string> reference_to;
};

enum MergedConfigStatus {
  UNDEFINED,
  BUILDING,
  OK
};

struct merged_config_t {
  MergedConfigStatus status{MergedConfigStatus::UNDEFINED};
  int64_t last_access_timestamp{0};
  ElementRef value{nullptr};
  std::shared_ptr<::mhconfig::api::config::MergedConfig> api_merged_config{nullptr};
};


struct document_metadata_t {
  std::unordered_map<
    std::string,
    std::map<uint32_t, std::shared_ptr<raw_config_t>>
  > raw_config_by_version_by_override;

  std::unordered_map<std::string, ::jmutils::zero_value_t<uint32_t>> referenced_by;
};


struct merged_config_metadata_t {
  std::unordered_map<
    std::string,
    std::weak_ptr<merged_config_t>
  > merged_config_by_document;
};


// TODO move to the builder file
namespace build {
  struct build_element_t {
    mhconfig::ElementRef config;

    std::string name;
    std::string overrides_key;

    std::unordered_map<
      std::string,
      std::shared_ptr<raw_config_t>
    > raw_config_by_override;
  };

  struct built_element_t {
    std::string overrides_key;
    mhconfig::ElementRef config;
  };

  struct wait_built_t {
    bool is_main;
    std::unordered_map<std::string, uint32_t> pending_element_position_by_name;

    ::mhconfig::api::request::GetRequest* request;
    uint32_t specific_version;
    std::vector<build_element_t> elements_to_build;
  };
}

struct config_namespace_t {
  bool ok;
  uint32_t next_raw_config_id{1};
  uint32_t current_version{1};
  uint64_t id;
  uint64_t last_access_timestamp;
  std::string root_path;

  std::shared_ptr<string_pool::Pool> pool;

  std::unordered_map<
    std::string,
    std::shared_ptr<document_metadata_t>
  > document_metadata_by_document;

  std::unordered_map<
    std::string,
    std::shared_ptr<merged_config_metadata_t>
  > merged_config_metadata_by_overrides_key;

  std::vector<
    std::shared_ptr<merged_config_t>
  > merged_config_by_gc_generation[NUMBER_OF_GC_GENERATIONS];

  std::unordered_map<
    std::string,
    std::vector<std::shared_ptr<build::wait_built_t>>
  > wait_builts_by_key;

  std::list<std::pair<uint64_t, uint32_t>> stored_versions_by_deprecation_timestamp;
};

} /* config_namespace */
} /* ds */
} /* mhconfig */

#endif
