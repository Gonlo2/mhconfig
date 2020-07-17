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
    const std::string& overrides_key,
    std::shared_ptr<inja::Template>&& template_
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

  inline bool add_overrides_key(
    config_namespace_t& config_namespace,
    std::string& overrides_key,
    std::shared_ptr<inja::Template>& template_
  ) {
    for_each_document_override(
      config_namespace,
      get_request_->flavors(),
      get_request_->overrides(),
      get_request_->document(),
      get_request_->version(),
      [&overrides_key](const auto&, auto& raw_config) {
        jmutils::push_uint32(overrides_key, raw_config->id);
      }
    );

    if (!get_request_->template_().empty()) {
      thread_local static std::string override_path;
      template_ = nullptr;
      for (size_t i = get_request_->overrides().size(); (template_ == nullptr) && i--;) {
        make_override_path(
          get_request_->overrides()[i],
          get_request_->template_(),
          "",
          override_path
        );
        with_raw_config(
          config_namespace,
          override_path,
          get_request_->version(),
          [&template_, &overrides_key](const auto&, auto& raw_config) {
            template_ = raw_config->template_;
            jmutils::push_uint32(overrides_key, raw_config->id);
          }
        );
      }
      if (template_ == nullptr) {
        spdlog::warn(
          "Can't found a template file with the name '{}'",
          get_request_->template_()
        );
        get_request_->set_status(api::request::GetRequest::Status::ERROR);
        return false;
      }
    }

    return true;
  }

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
