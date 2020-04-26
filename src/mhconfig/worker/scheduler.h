#ifndef MHCONFIG__WORKER__SCHEDULER_H
#define MHCONFIG__WORKER__SCHEDULER_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
//#include "jmutils/metrics/scope_duration.h"
#include "string_pool/pool.h"
#include "mhconfig/worker/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/api/config/basic_merged_config.h"
#include "mhconfig/api/config/optimized_merged_config.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{
namespace worker
{

using jmutils::container::Queue;
using namespace mhconfig::api::request;

class Scheduler
{
public:
  Scheduler(
    Queue<command::command_t>& scheduler_queue,
    Queue<command::command_t>& worker_queue,
    Metrics& metrics
  );

  virtual ~Scheduler();

  bool start();
  void join();

private:
  enum ConfigNamespaceState {
    OK,
    BUILDING,
    ERROR
  };

  std::shared_ptr<spdlog::logger> logger_{spdlog::get("console")};

  Queue<command::command_t>& scheduler_queue_;
  Queue<command::command_t>& worker_queue_;

  std::unique_ptr<std::thread> thread_;

  Metrics& metrics_;

  std::unordered_map<
    std::string,
    std::shared_ptr<config_namespace_t>
  > config_namespace_by_root_path_;

  std::unordered_map<
    uint64_t,
    std::shared_ptr<config_namespace_t>
  > config_namespace_by_id_;

  std::unordered_map<
    std::string,
    std::vector<command::command_t>
  > commands_waiting_for_config_namespace_by_root_path_;

  void run();

  bool process_command(command::command_t& command);

  bool process_command_type_get_request(
    get_request::GetRequest* get_request,
    std::shared_ptr<config_namespace_t> config_namespace
  );

  bool process_command_type_update_request(
    update_request::UpdateRequest* update_request,
    std::shared_ptr<config_namespace_t> config_namespace
  );

  std::shared_ptr<merged_config_t> get_merged_config(
    std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::string& overrides_key
  );

  std::shared_ptr<merged_config_t> get_or_build_merged_config(
    std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::string& overrides_key
  );

  std::pair<ConfigNamespaceState, std::shared_ptr<config_namespace_t>> get_or_build_config_namespace(
    const std::string& root_path,
    const command::command_t& command
  );

  bool send_api_get_response(
    get_request::GetRequest* get_request,
    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  );

  bool send_api_response(
    Request* api_request
  );

  bool process_command_type_run_gc_request(
    const std::shared_ptr<command::run_gc::request_t> run_gc_request
  );

  bool remove_merge_configs(
    uint32_t limit_timestamp,
    uint32_t generation
  );

  bool remove_dead_pointers();
  bool remove_namespaces(uint32_t limit_timestamp);
  bool remove_versions(uint32_t limit_timestamp);

  bool process_command_type_update_response(
    const std::shared_ptr<command::update::response_t> update_response,
    std::shared_ptr<config_namespace_t> config_namespace
  );

  void get_affected_documents(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    std::unordered_set<std::string>& affected_documents
  );

  bool send_update_response(
    void* id,
    uint64_t namespace_id,
    command::update::ResponseStatus status,
    uint32_t version
  );

  bool prepare_build_request(
    get_request::GetRequest* get_request,
    std::shared_ptr<config_namespace_t> config_namespace
  );

  bool process_command_type_setup_response(
    const std::shared_ptr<command::setup::response_t> setup_response
  );

  void reset_ids_of_namespace(
    std::shared_ptr<config_namespace_t> config_namespace
  );

  bool process_command_type_build_response(
    const std::shared_ptr<command::build::response_t> build_response
  );

  std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> check_if_ref_graph_is_a_dag(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::vector<std::string> overrides,
    uint32_t version
  );

  bool check_if_ref_graph_is_a_dag_rec(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::vector<std::string> overrides,
    uint32_t version,
    std::vector<std::string>& dfs_path,
    std::unordered_set<std::string>& dfs_path_set,
    std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
  );

  std::vector<std::string> do_topological_sort_over_ref_graph(
    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
  );

  void do_topological_sort_over_ref_graph_rec(
    const std::string& document,
    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents,
    std::unordered_set<std::string>& visited_documents,
    std::vector<std::string>& inverted_topological_sort
  );

  std::shared_ptr<raw_config_t> get_raw_config(
    const std::shared_ptr<document_metadata_t> document_metadata,
    const std::string& override_,
    uint32_t version
  );

  std::string make_overrides_key(
    const std::shared_ptr<document_metadata_t> document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version
  );

  inline uint32_t get_specific_version(
    const std::shared_ptr<config_namespace_t> config_namespace,
    uint32_t version
  );

};

} /* worker */
} /* mhconfig */


#endif
