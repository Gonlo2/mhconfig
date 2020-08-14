#ifndef MHCONFIG__CONFIG_NAMESPACE_H
#define MHCONFIG__CONFIG_NAMESPACE_H

#include <vector>
#include <map>
#include <string>
#include <memory>
#include <deque>

#include <boost/functional/hash.hpp>

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/element.h"

#include "jmutils/container/weak_container.h"
#include "jmutils/container/weak_multimap.h"
#include "jmutils/string/pool.h"
#include "jmutils/common.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_map.h>
#include <absl/synchronization/mutex.h>

namespace mhconfig
{

//TODO Review the names

typedef uint16_t DocumentId;
typedef uint16_t RawConfigId;
typedef uint16_t VersionId;

const static uint8_t NUMBER_OF_MC_GENERATIONS{3};

struct raw_config_t {
  bool has_content{false};
  RawConfigId id;
  uint64_t checksum;
  Element value;
  std::vector<std::string> reference_to;

  std::shared_ptr<raw_config_t> clone() {
    auto result = std::make_shared<raw_config_t>();
    result->has_content = has_content;
    result->id = id;
    result->checksum = checksum;
    result->value = value;
    result->reference_to = reference_to;
    return result;
  }
};

enum class MergedConfigStatus {
  UNDEFINED,
  BUILDING,
  OK_CONFIG_NO_OPTIMIZED,
  OK_CONFIG_OPTIMIZING,
  OK_CONFIG_OPTIMIZED,
  REF_GRAPH_IS_NOT_DAG,
  INVALID_VERSION
};

struct merged_config_t;
struct document_t;

struct build_element_t {
  bool to_build{false};
  mhconfig::Element config;
  std::shared_ptr<document_t> document;
  std::shared_ptr<merged_config_t> merged_config;
  std::vector<std::shared_ptr<raw_config_t>> raw_configs_to_merge;
  std::vector<std::unique_ptr<build_element_t>> children;
};

struct pending_build_t {
  std::atomic<uint32_t> num_pending;
  VersionId version;
  std::shared_ptr<api::request::GetRequest> request;
  std::unique_ptr<build_element_t> element;
};

struct merged_config_t {
  absl::Mutex mutex;
  MergedConfigStatus status : 8;
  uint64_t creation_timestamp : 56;
  uint64_t last_access_timestamp;
  Element value;
  // The preprocesed_value field store the optimized value
  std::string preprocesed_value;

  // Change this to a shared_ptr (?)
  absl::flat_hash_set<std::string> reference_to;

  std::vector<std::shared_ptr<pending_build_t>> to_build;
  std::vector<std::shared_ptr<api::request::GetRequest>> waiting;

  std::shared_ptr<merged_config_t> next;

  merged_config_t()
    : status(MergedConfigStatus::UNDEFINED),
    creation_timestamp(0),
    last_access_timestamp(0)
  {}
};

struct override_t {
  absl::btree_map<VersionId, std::shared_ptr<raw_config_t>> raw_config_by_version;
};

struct merged_config_generation_t {
  absl::Mutex gc_mutex;
  std::shared_ptr<merged_config_t> head;
};

struct document_t {
  absl::Mutex mutex;
  DocumentId id;
  RawConfigId next_raw_config_id{0};
  VersionId oldest_version;

  absl::flat_hash_map<std::string, override_t> override_by_path;

  absl::flat_hash_map<
    std::string,
    std::weak_ptr<merged_config_t>
  > merged_config_by_overrides_key;

  std::string name;

  merged_config_generation_t mc_generation[NUMBER_OF_MC_GENERATIONS];
};

struct document_versions_t {
  absl::Mutex mutex;
  absl::btree_map<VersionId, std::shared_ptr<document_t>> document_by_version;
  jmutils::container::WeakContainer<api::stream::WatchInputMessage> watchers;
  absl::flat_hash_map<std::string, ::jmutils::zero_value_t<uint32_t>> referenced_by;
};

enum class ConfigNamespaceStatus : uint8_t {
  UNDEFINED,
  BUILDING,
  OK,
  OK_UPDATING,
  DELETED
};

struct config_namespace_t {
  absl::Mutex mutex;
  uint64_t last_access_timestamp;
  ConfigNamespaceStatus status{ConfigNamespaceStatus::UNDEFINED};
  DocumentId next_document_id{0};
  VersionId oldest_version{1};
  VersionId current_version{1};
  uint64_t id;

  absl::flat_hash_map<std::string, std::unique_ptr<document_versions_t>> document_versions_by_name;

  // Start trace structures
  jmutils::container::WeakMultimap<std::string, api::stream::TraceInputMessage> traces_by_override;
  jmutils::container::WeakMultimap<std::string, api::stream::TraceInputMessage> traces_by_flavor;
  jmutils::container::WeakMultimap<std::string, api::stream::TraceInputMessage> traces_by_document;
  jmutils::container::WeakContainer<api::stream::TraceInputMessage> to_trace_always;
  // End trace structures

  // Rarely accessed datastructures
  std::string root_path;
  std::shared_ptr<jmutils::string::Pool> pool;

  absl::flat_hash_set<DocumentId> document_ids_in_use;

  std::deque<std::pair<VersionId, uint64_t>> stored_versions;

  std::vector<std::shared_ptr<api::request::GetRequest>> get_requests_waiting;
  std::deque<std::shared_ptr<api::request::UpdateRequest>> update_requests_waiting;
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watch_requests_waiting;
  std::vector<std::shared_ptr<api::stream::TraceInputMessage>> trace_requests_waiting;
};

} /* mhconfig */

#endif
