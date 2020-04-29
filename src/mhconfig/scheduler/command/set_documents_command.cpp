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
  std::shared_ptr<config_namespace_t> config_namespace,
  Queue<worker::command::CommandRef>& worker_queue
) {
  spdlog::debug(
    "Processing the build response with id {}",
    (uint64_t) wait_build_->request
  );

  for (const auto it : built_elements_by_document_) {
    spdlog::debug("Setting the document '{}'", it.first);

    auto document_metadata = config_namespace->document_metadata_by_document[it.first];
    auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
      config_namespace,
      it.first,
      it.second.overrides_key
    );
    merged_config->status = MergedConfigStatus::OK;
    merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();
    merged_config->value = it.second.config;
    merged_config->api_merged_config = std::make_shared<mhconfig::api::config::BasicMergedConfig>(it.second.config);

    std::stringstream wait_built_key_ss;
    wait_built_key_ss << it.first << ':' << it.second.overrides_key;

    auto& wait_builts = config_namespace->wait_builts_by_key[wait_built_key_ss.str()];
    for (size_t i = 0; i < wait_builts.size(); ) {
      auto wait_built = wait_builts[i];

      spdlog::debug(
        "Unchecking element built for the request with id {}",
        (uint64_t) wait_built->request
      );
      auto search = wait_built->pending_element_position_by_name.find(it.first);
      wait_built->elements_to_build[search->second].config = it.second.config;
      wait_built->pending_element_position_by_name.erase(search);

      if (wait_built->pending_element_position_by_name.empty()) {
        if (wait_built->is_main) {
          spdlog::debug(
            "Responding the get request with id {}",
            (uint64_t) wait_built->request
          );

          //send_api_get_response(other_get_request, merged_config->api_merged_config);
        } else {
          spdlog::debug(
            "Sending the get request with id {} to built",
            (uint64_t) wait_built->request
          );

          auto build_command = std::make_shared<::mhconfig::worker::command::BuildCommand>(
            config_namespace->id,
            config_namespace->pool,
            wait_built
          );
          worker_queue.push(build_command);
        }

        // We do a swap delete because we don't care of the order and it's faster :P
        wait_builts[i] = wait_builts.back();
        wait_builts.pop_back();
      } else {
        ++i;
      }
    }
  }

  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    wait_build_->request->key()[0],
    built_elements_by_document_[wait_build_->request->key()[0]].overrides_key
  );

  //FIXME
  // send_api_get_response(get_request, merged_config->api_merged_config);
  auto api_reply_command = std::make_shared<::mhconfig::worker::command::ApiReplyCommand>(
    static_cast<::mhconfig::api::request::Request*>(wait_build_->request)
  );
  worker_queue.push(api_reply_command);

  return NamespaceExecutionResult::OK;
}

bool SetDocumentsCommand::on_get_namespace_error(
  Queue<worker::command::CommandRef>& worker_queue
) {
  assert(false);
}

} /* command */
} /* scheduler */
} /* mhconfig */
