#ifndef MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/worker/command/api_reply_command.h"
#include "mhconfig/worker/command/api_get_reply_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace ::mhconfig::builder;

class ApiGetCommand : public Command
{
public:
  ApiGetCommand(
    std::shared_ptr<::mhconfig::api::request::GetRequest> get_request
  );
  virtual ~ApiGetCommand();

  std::string name() const override;

  CommandType command_type() const override;
  const std::string& namespace_path() const override;

  NamespaceExecutionResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  std::shared_ptr<::mhconfig::api::request::GetRequest> get_request_;

  void send_api_response(
    WorkerQueue& worker_queue
  );

  void send_api_get_response(
    WorkerQueue& worker_queue,
    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  );

  NamespaceExecutionResult prepare_build_request(
    config_namespace_t& config_namespace,
    WorkerQueue& worker_queue
  );

  std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> check_if_ref_graph_is_a_dag(
    config_namespace_t& config_namespace,
    const std::string& document,
    const std::vector<std::string>& overrides,
    uint32_t version
  );

  bool check_if_ref_graph_is_a_dag_rec(
    config_namespace_t& config_namespace,
    const std::string& document,
    const std::vector<std::string>& overrides,
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

  // Help functions
  inline void make_overrides_key(
    const document_metadata_t& document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version,
    std::string& overrides_key
  ) {
    overrides_key.clear();
    overrides_key.reserve(overrides.size()*4);
    for (auto& override_: overrides) {
      auto raw_config = get_raw_config(document_metadata, override_, version);
      if (raw_config != nullptr) {
        jmutils::push_uint32(overrides_key, raw_config->id);
      }
    }
  }

  inline uint32_t get_specific_version(
    const config_namespace_t& config_namespace,
    uint32_t version
  ) {
    return (version == 0) ? config_namespace.current_version : version;
  }
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
