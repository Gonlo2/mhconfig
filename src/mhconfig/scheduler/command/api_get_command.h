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

  NamespaceExecutionResult prepare_build_request(
    config_namespace_t& config_namespace,
    WorkerQueue& worker_queue,
    const std::string& overrides_key,
    std::shared_ptr<inja::Template>&& template_
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
  inline void add_config_overrides_key(
    const document_metadata_t* document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version,
    std::string& overrides_key
  ) {
    for (size_t i = overrides.size(); i--;) {
      with_raw_config(
        document_metadata,
        overrides[i],
        version,
        [&overrides_key](auto& raw_config) {
          jmutils::push_uint32(overrides_key, raw_config->id);
        }
      );
    }
  }

  inline void add_template_overrides_key(
    const document_metadata_t* document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version,
    std::string& overrides_key,
    std::shared_ptr<inja::Template>& template_
  ) {
    template_ = nullptr;
    for (size_t i = overrides.size(); (template_ == nullptr) && i--;) {
      with_raw_config(
        document_metadata,
        overrides[i],
        version,
        [&](auto& raw_config) {
          template_ = raw_config->template_;
          jmutils::push_uint32(overrides_key, raw_config->id);
        }
      );
    }
  }

  inline bool add_overrides_key(
    config_namespace_t& config_namespace,
    std::string& overrides_key,
    std::shared_ptr<inja::Template>& template_
  ) {
    if (!get_request_->template_().empty()) {
      auto search = config_namespace.document_metadata_by_document
        .find(get_request_->template_());

      template_ = nullptr;
      if (search != config_namespace.document_metadata_by_document.end()) {
        add_template_overrides_key(
          search->second.get(),
          get_request_->overrides(),
          get_request_->version(),
          overrides_key,
          template_
        );
      }
      if (template_ == nullptr) {
        spdlog::warn(
          "Can't found a template file with the name '{}'",
          get_request_->template_()
        );
        get_request_->set_status(
          ::mhconfig::api::request::GetRequest::Status::ERROR
        );
        return false;
      }
    }

    auto search = config_namespace.document_metadata_by_document
      .find(get_request_->document());

    if (search == config_namespace.document_metadata_by_document.end()) {
      spdlog::warn(
        "Can't found a config file with the name '{}'",
        get_request_->document()
      );
      get_request_->set_element(UNDEFINED_ELEMENT);
      return false;
    }
    add_config_overrides_key(
      search->second.get(),
      get_request_->overrides(),
      get_request_->version(),
      overrides_key
    );

    return true;
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
