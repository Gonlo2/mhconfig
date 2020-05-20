#include "mhconfig/scheduler/command/set_documents_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

SetDocumentsCommand::SetDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<build::wait_built_t> wait_build,
  std::unordered_map<std::string, build::built_element_t> built_elements_by_document
)
  : Command(),
  namespace_id_(namespace_id),
  wait_build_(wait_build),
  built_elements_by_document_(built_elements_by_document)
{
}

SetDocumentsCommand::~SetDocumentsCommand() {
}

std::string SetDocumentsCommand::name() const {
  return "SET_DOCUMENTS";
}

CommandType SetDocumentsCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_ID;
}

uint64_t SetDocumentsCommand::namespace_id() const {
  return namespace_id_;
}

NamespaceExecutionResult SetDocumentsCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  spdlog::debug(
    "Processing the build response with id {}",
    (void*) wait_build_->request.get()
  );

  for (const auto it : built_elements_by_document_) {
    spdlog::debug("Setting the document '{}'", it.first);

    auto document_metadata = config_namespace.document_metadata_by_document[it.first];
    auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
      config_namespace,
      it.first,
      it.second.overrides_key
    );
    merged_config->status = MergedConfigStatus::OK;
    merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();
    merged_config->value = it.second.config;
    merged_config->api_merged_config = std::make_shared<mhconfig::api::config::BasicMergedConfig>(it.second.config);

    auto wait_builts_search = config_namespace.wait_builts_by_key
      .find(it.second.overrides_key);

    if (wait_builts_search != config_namespace.wait_builts_by_key.end()) {
      for (size_t i = 0; i < wait_builts_search->second.size(); ) {
        auto wait_built = wait_builts_search->second[i];

        spdlog::debug(
          "Unchecking element built for the request with id {}",
          (void*) wait_built->request.get()
        );
        auto search = wait_built->pending_element_position_by_name.find(it.first);
        wait_built->elements_to_build[search->second].config = it.second.config;
        wait_built->pending_element_position_by_name.erase(search);

        if (wait_built->pending_element_position_by_name.empty()) {
          if (wait_built->is_main) {
            spdlog::debug(
              "Responding the get request with id {}",
              (void*) wait_built->request.get()
            );

            worker_queue.push(
              std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
                std::move(wait_built->request),
                merged_config->api_merged_config
              )
            );
          } else {
            spdlog::debug(
              "Sending the get request with id {} to built",
              (void*) wait_built->request.get()
            );

            worker_queue.push(
              std::make_unique<::mhconfig::worker::command::BuildCommand>(
                config_namespace.id,
                config_namespace.pool,
                wait_built
              )
            );
          }

          jmutils::swap_delete(wait_builts_search->second, i);
        } else {
          ++i;
        }
      }
      if (wait_builts_search->second.empty()) {
        config_namespace.wait_builts_by_key.erase(wait_builts_search);
      }
    }
  }

  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    wait_build_->request->document(),
    built_elements_by_document_[wait_build_->request->document()].overrides_key
  );

  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
      std::move(wait_build_->request),
      merged_config->api_merged_config
    )
  );

  return NamespaceExecutionResult::OK;
}

bool SetDocumentsCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  wait_build_->request->set_element(UNDEFINED_ELEMENT);

  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(wait_build_->request)
    )
  );

  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
