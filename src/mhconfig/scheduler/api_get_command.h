#ifndef MHCONFIG__SCHEDULER__API_GET_COMMAND_H
#define MHCONFIG__SCHEDULER__API_GET_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/build_command.h"
#include "mhconfig/worker/api_reply_command.h"
#include "mhconfig/worker/api_batch_reply_command.h"
#include "mhconfig/worker/api_get_reply_command.h"
#include "mhconfig/api/stream/trace_stream_impl.h"
#include "mhconfig/scheduler/common.h"
#include "mhconfig/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{

using namespace ::mhconfig::builder;

class ApiGetCommand : public SchedulerCommand
{
public:
  ApiGetCommand(
    std::shared_ptr<api::request::GetRequest>&& get_request
  );
  virtual ~ApiGetCommand();

  std::string name() const override;

  CommandType type() const override;
  const std::string& namespace_path() const override;

  CommandResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  std::shared_ptr<::mhconfig::api::request::GetRequest> get_request_;

  bool validate_request(
    const config_namespace_t& config_namespace,
    WorkerQueue& worker_queue
  );

  void send_api_response(
    WorkerQueue& worker_queue
  );

  CommandResult prepare_build_request(
    config_namespace_t& config_namespace,
    WorkerQueue& worker_queue,
    const std::string& overrides_key
  );

  bool check_if_ref_graph_is_a_dag(
    config_namespace_t& config_namespace,
    std::vector<std::string>& topological_sort,
    absl::flat_hash_map<std::string, std::shared_ptr<raw_config_t>>& raw_config_by_override_path,
    absl::flat_hash_map<std::string, std::string>& overrides_key_by_document
  );

  // TODO Move to the builder
  bool check_if_ref_graph_is_a_dag_rec(
    config_namespace_t& config_namespace,
    const std::vector<std::string>& flavors,
    const std::vector<std::string>& overrides,
    const std::string& document,
    uint32_t version,
    absl::flat_hash_set<std::string>& dfs_path_set,
    std::vector<std::string>& topological_sort,
    absl::flat_hash_map<std::string, std::shared_ptr<raw_config_t>>& raw_config_by_override_path,
    absl::flat_hash_map<std::string, std::string>& overrides_key_by_document
  );

  inline uint32_t get_specific_version(
    const config_namespace_t& config_namespace,
    uint32_t version
  ) {
    return (version == 0) ? config_namespace.current_version : version;
  }
};

} /* scheduler */
} /* mhconfig */

#endif
