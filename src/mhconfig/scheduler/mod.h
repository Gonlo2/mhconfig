#ifndef MHCONFIG__SCHEDULER__MOD_H
#define MHCONFIG__SCHEDULER__MOD_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "string_pool/pool.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/api/config/basic_merged_config.h"
#include "mhconfig/api/config/optimized_merged_config.h"
#include "mhconfig/worker/command/setup_command.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{
namespace scheduler
{

using jmutils::container::Queue;
using namespace mhconfig::api::request;

class Scheduler : public jmutils::parallelism::Worker<Scheduler, command::CommandRef>
{
public:
  Scheduler(
    Queue<command::CommandRef>& scheduler_queue,
    Queue<mhconfig::worker::command::CommandRef>& worker_queue,
    Metrics& metrics
  );

  virtual ~Scheduler();

private:
  enum ConfigNamespaceState {
    OK,
    BUILDING,
    ERROR
  };

  Queue<mhconfig::worker::command::CommandRef>& worker_queue_;

  Metrics& metrics_;

  std::unordered_map<std::string, std::shared_ptr<config_namespace_t>> namespace_by_path_;
  std::unordered_map<uint64_t, std::shared_ptr<config_namespace_t>> namespace_by_id_;

  std::unordered_map<std::string, std::vector<command::CommandRef>> commands_waiting_for_namespace_by_path_;


  void loop_stats(
    command::CommandRef command,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  );

  bool process_command(
    command::CommandRef command
  );

  std::pair<ConfigNamespaceState, std::shared_ptr<config_namespace_t>> get_or_build_namespace(
    command::CommandRef command
  );
//
//  void get_affected_documents(
//    const std::shared_ptr<config_namespace_t> config_namespace,
//    const std::string& document,
//    std::unordered_set<std::string>& affected_documents
//  );
//
//  bool send_update_response(
//    void* id,
//    uint64_t namespace_id,
//    command::update::ResponseStatus status,
//    uint32_t version
//  );
//
//  bool prepare_build_request(
//    get_request::GetRequest* get_request,
//    std::shared_ptr<config_namespace_t> config_namespace
//  );
//
//  void reset_ids_of_namespace(
//    std::shared_ptr<config_namespace_t> config_namespace
//  );
//
//  std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> check_if_ref_graph_is_a_dag(
//    const std::shared_ptr<config_namespace_t> config_namespace,
//    const std::string& document,
//    const std::vector<std::string> overrides,
//    uint32_t version
//  );
//
//  bool check_if_ref_graph_is_a_dag_rec(
//    const std::shared_ptr<config_namespace_t> config_namespace,
//    const std::string& document,
//    const std::vector<std::string> overrides,
//    uint32_t version,
//    std::vector<std::string>& dfs_path,
//    std::unordered_set<std::string>& dfs_path_set,
//    std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
//  );
//
//  std::vector<std::string> do_topological_sort_over_ref_graph(
//    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
//  );
//
//  void do_topological_sort_over_ref_graph_rec(
//    const std::string& document,
//    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents,
//    std::unordered_set<std::string>& visited_documents,
//    std::vector<std::string>& inverted_topological_sort
//  );
//
//  std::shared_ptr<raw_config_t> get_raw_config(
//    const std::shared_ptr<document_metadata_t> document_metadata,
//    const std::string& override_,
//    uint32_t version
//  );
//
//  std::string make_overrides_key(
//    const std::shared_ptr<document_metadata_t> document_metadata,
//    const std::vector<std::string>& overrides,
//    uint32_t version
//  );
//
//  inline uint32_t get_specific_version(
//    const std::shared_ptr<config_namespace_t> config_namespace,
//    uint32_t version
//  );
//
};

} /* scheduler */
} /* mhconfig */


#endif
