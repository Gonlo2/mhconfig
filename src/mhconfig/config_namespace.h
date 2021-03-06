#ifndef MHCONFIG__CONFIG_NAMESPACE_H
#define MHCONFIG__CONFIG_NAMESPACE_H

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <atomic>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "jmutils/common.h"
#include "jmutils/container/label_set.h"
#include "jmutils/container/queue.h"
#include "jmutils/container/weak_container.h"
#include "jmutils/container/weak_multimap.h"
#include "jmutils/container/linked_list.h"
#include "jmutils/string/pool.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/auth/policy.h"
#include "mhconfig/auth/tokens.h"
#include "mhconfig/element.h"
#include "mhconfig/metrics.h"
#include "mhconfig/logger/logger.h"
#include "mhconfig/logger/persistent_logger.h"
#include "mhconfig/logger/multi_persistent_logger.h"
#include "mhconfig/constants.h"

namespace mhconfig
{

using jmutils::container::Labels;
using jmutils::container::EMPTY_LABELS;
using jmutils::container::LabelSet;
using jmutils::container::label_t;
using jmutils::container::WeakContainer;
using jmutils::container::WeakMultimap;
using jmutils::container::LinkedList;
using logger::Logger;
using logger::ReplayLogger;
using logger::PersistentLogger;
using logger::MultiPersistentLogger;

//TODO Review the names

struct raw_config_t {
  bool has_content{false};
  RawConfigId id;
  uint32_t file_checksum{0};
  Element value;
  std::shared_ptr<PersistentLogger> logger;
  std::vector<std::string> reference_to;
  uint64_t checksum{0};
  std::string path;

  std::shared_ptr<raw_config_t> clone() {
    auto result = std::make_shared<raw_config_t>();
    result->has_content = has_content;
    result->id = id;
    result->file_checksum = file_checksum;
    result->value = value;
    result->logger = logger;
    result->reference_to = reference_to;
    result->checksum = checksum;
    result->path = path;
    return result;
  }
};

class GetConfigTask
{
public:
  GetConfigTask() {};
  virtual ~GetConfigTask() {};

  virtual const std::string& root_path() const = 0;
  virtual uint32_t version() const = 0;
  virtual const Labels& labels() const = 0;
  virtual const std::string& document() const = 0;

  virtual void on_complete(
    std::shared_ptr<config_namespace_t>& cn,
    VersionId version,
    const Element& element,
    const std::array<uint8_t, 32>& checksum,
    void* payload
  ) = 0;

  virtual Logger& logger() = 0;

  virtual ReplayLogger::Level log_level() const = 0;
};

class PolicyCheck
{
public:
  PolicyCheck() {};
  virtual ~PolicyCheck() {};

  virtual void on_check_policy(
    auth::AuthResult auth_result,
    auth::Policy* policy
  ) = 0;

  virtual void on_check_policy_error() = 0;
};

enum class MergedConfigStatus : uint8_t {
  UNDEFINED,
  BUILDING,
  NO_OPTIMIZED,
  OPTIMIZING,
  OPTIMIZED,
  OPTIMIZATION_FAIL
};

struct document_t;
struct merged_config_t;

struct build_element_t {
  bool to_build{false};
  mhconfig::Element config;
  std::shared_ptr<document_t> document;
  std::shared_ptr<merged_config_t> merged_config;
  LinkedList<std::shared_ptr<raw_config_t>> raw_configs_to_merge;
};

struct pending_build_t {
  std::atomic<uint32_t> num_pending;
  VersionId version;
  std::shared_ptr<GetConfigTask> task;
  LinkedList<build_element_t> elements;
};

struct merged_config_payload_fun_t {
  bool (*alloc)(Element&, void*&);
  void (*dealloc)(void*);
};

struct merged_config_t {
  absl::Mutex mutex;
  MergedConfigStatus status : 8;
  uint64_t creation_timestamp : 56;
  uint64_t last_access_timestamp;
  Element value;
  std::array<uint8_t, 32> checksum;

  MultiPersistentLogger logger;

  void* payload;
  merged_config_payload_fun_t payload_fun;

  // Change this to a shared_ptr (?)
  absl::flat_hash_set<std::string> reference_to;

  std::vector<std::shared_ptr<pending_build_t>> to_build;
  std::vector<std::shared_ptr<GetConfigTask>> waiting;

  std::shared_ptr<merged_config_t> next;

  merged_config_t()
    : status(MergedConfigStatus::UNDEFINED),
    creation_timestamp(0),
    last_access_timestamp(0),
    checksum(UNDEFINED_ELEMENT_CHECKSUM),
    payload(nullptr)
  {}

  ~merged_config_t() {
    if (payload != nullptr) {
      payload_fun.dealloc(payload);
    }
  }
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

  LabelSet<override_t> lbl_set;
  absl::flat_hash_map<RawConfigId, std::shared_ptr<raw_config_t>> raw_config_by_id;

  absl::flat_hash_map<
    std::string,
    std::weak_ptr<merged_config_t>
  > merged_config_by_overrides_key;

  merged_config_payload_fun_t mc_payload_fun;

  std::string name;

  merged_config_generation_t mc_generation[NUMBER_OF_MC_GENERATIONS];
};

struct document_versions_t {
  absl::Mutex mutex;
  absl::btree_map<VersionId, std::shared_ptr<document_t>> document_by_version;
  WeakContainer<api::stream::WatchInputMessage> watchers;
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

  absl::flat_hash_map<
    std::string,
    std::unique_ptr<document_versions_t>
  > document_versions_by_name;

  // Start trace structures
  WeakMultimap<label_t, api::stream::TraceInputMessage> traces_by_label;
  WeakMultimap<std::string, api::stream::TraceInputMessage> traces_by_document;
  WeakContainer<api::stream::TraceInputMessage> to_trace_always;
  // End trace structures

  // Rarely accessed datastructures
  std::string root_path;
  std::shared_ptr<jmutils::string::Pool> pool;

  absl::flat_hash_map<std::string, merged_config_payload_fun_t> mc_payload_fun_by_document;
  absl::flat_hash_map<DocumentId, std::shared_ptr<document_t>> document_by_id;

  std::deque<std::pair<VersionId, uint64_t>> stored_versions;

  std::vector<std::shared_ptr<GetConfigTask>> get_config_tasks_waiting;
  std::deque<std::shared_ptr<api::request::UpdateRequest>> update_requests_waiting;
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watch_requests_waiting;
  std::vector<std::shared_ptr<api::stream::TraceInputMessage>> trace_requests_waiting;
};

class WorkerCommand;

typedef std::unique_ptr<WorkerCommand> WorkerCommandRef;

typedef jmutils::container::Queue<WorkerCommandRef> WorkerQueue;

struct context_t {
  absl::Mutex mutex;
  absl::flat_hash_map<std::string, std::shared_ptr<config_namespace_t>> cn_by_root_path;
  WorkerQueue worker_queue;
  Metrics metrics;
  std::string mhc_root_path;
};

class WorkerCommand
{
public:
  virtual ~WorkerCommand();
  virtual std::string name() const = 0;
  virtual bool force_take_metric() const;
  virtual bool execute(context_t* context) = 0;
};

} /* mhconfig */

#endif
