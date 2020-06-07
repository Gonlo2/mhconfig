#ifndef MHCONFIG__DS__CONFIG_NAMESPACE_H
#define MHCONFIG__DS__CONFIG_NAMESPACE_H

#include <vector>
#include <map>
#include <string>
#include <memory>

#include <boost/functional/hash.hpp>

#include <inja/inja.hpp>

#include "string_pool/pool.h"

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/element.h"
#include "jmutils/common.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_map.h>

namespace mhconfig
{
namespace ds
{
namespace config_namespace
{

//TODO Review the names

const static uint8_t NUMBER_OF_GC_GENERATIONS{3};

struct raw_config_t {
  uint32_t id{0};
  uint32_t crc32{0};
  Element value;
  std::shared_ptr<inja::Template> template_{nullptr};
  std::vector<std::string> reference_to;

  std::shared_ptr<raw_config_t> clone(uint32_t new_id) {
    auto result = std::make_shared<raw_config_t>();
    result->id = new_id;
    result->crc32 = crc32;
    result->value = value;
    result->template_ = template_;
    result->reference_to = reference_to;
    return result;
  }
};

enum MergedConfigStatus {
  UNDEFINED,
  BUILDING,
  OK_CONFIG_NO_OPTIMIZED,
  OK_CONFIG_OPTIMIZING,
  OK_CONFIG_OPTIMIZED,
  OK_TEMPLATE
};

struct merged_config_t {
  MergedConfigStatus status : 8;
  uint64_t creation_timestamp : 56;
  uint64_t last_access_timestamp;
  Element value;
  // The preprocesed_value field store the rendered template or the optimized value
  std::string preprocesed_value;

  merged_config_t()
    : status(MergedConfigStatus::UNDEFINED),
    creation_timestamp(0),
    last_access_timestamp(0)
  {}
};

struct override_metadata_t {
  absl::btree_map<uint32_t, std::shared_ptr<raw_config_t>> raw_config_by_version;
  std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers;
};

//TODO Check if it's better use a unique_ptr for this to avoid copy
// the data in the hash table rebuilds
struct document_metadata_t {
  absl::flat_hash_map<std::string, override_metadata_t> override_by_key;
  absl::flat_hash_map<std::string, ::jmutils::zero_value_t<uint32_t>> referenced_by;
};

// TODO move to the builder file
namespace build {
  struct build_element_t {
    mhconfig::Element config;

    std::string name;
    std::string overrides_key;
    bool to_build{true};

    absl::flat_hash_map<
      std::string,
      std::shared_ptr<raw_config_t>
    > raw_config_by_override;
  };

  struct wait_built_t {
    uint32_t specific_version;
    uint16_t num_pending_elements;
    bool is_preprocesed_value_ok;
    std::shared_ptr<::mhconfig::api::request::GetRequest> request;
    std::shared_ptr<inja::Template> template_;
    std::string overrides_key;
    std::string preprocesed_value;
    std::vector<build_element_t> elements_to_build;
  };
}

struct config_namespace_t {
  uint32_t next_raw_config_id{1};
  uint32_t current_version{1};
  uint64_t id;
  uint64_t last_access_timestamp : 56;
  bool ok : 8;
  std::string root_path;

  std::shared_ptr<::string_pool::Pool> pool;

  absl::flat_hash_map<std::string, document_metadata_t> document_metadata_by_document;

  absl::flat_hash_map<
    std::string,
    std::weak_ptr<merged_config_t>
  > merged_config_by_overrides_key;

  absl::flat_hash_map<
    std::pair<std::string, std::string>,
    ::jmutils::zero_value_t<uint32_t>
  > asked_configs;

  std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers;

  std::vector<
    std::shared_ptr<merged_config_t>
  > merged_config_by_gc_generation[NUMBER_OF_GC_GENERATIONS];

  absl::flat_hash_map<
    std::string,
    std::vector<std::shared_ptr<build::wait_built_t>>
  > wait_builts_by_key;

  std::list<std::pair<uint64_t, uint32_t>> stored_versions_by_deprecation_timestamp;
};

} /* config_namespace */
} /* ds */
} /* mhconfig */

#endif
