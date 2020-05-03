#include "mhconfig/scheduler/command/api_watch_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ApiWatchCommand::ApiWatchCommand(
  std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message
) : Command(),
    message_(message)
{
}

ApiWatchCommand::~ApiWatchCommand() {
}

std::string ApiWatchCommand::name() const {
  return "API_WATCH";
}

CommandType ApiWatchCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiWatchCommand::namespace_path() const {
  return message_->root_path();
}

NamespaceExecutionResult ApiWatchCommand::execute_on_namespace(
  std::shared_ptr<config_namespace_t> config_namespace,
  Queue<CommandRef>& scheduler_queue,
  Queue<worker::command::CommandRef>& worker_queue
) {
  // First we check if the asked version is lower that the current one
  if (config_namespace->current_version < message_->version()) {
    auto output_message = message_->make_output_message();
    output_message->set_uid(message_->uid());
    output_message->set_status(::mhconfig::api::stream::watch::Status::INVALID_VERSION);

    auto api_reply_command = std::make_shared<::mhconfig::worker::command::ApiReplyCommand>(
      output_message
    );
    worker_queue.push(api_reply_command);

    return NamespaceExecutionResult::OK;
  }

  bool notify = false;

  auto document_metadata = config_namespace
    ->document_metadata_by_document[message_->document()];

  for (const auto& override_: message_->overrides()) {
    auto& override_metadata = document_metadata->override_by_key[override_];
    //TODO Check if the overrides are distinct
    override_metadata.watchers.emplace_back(message_);
    notify |= !override_metadata.raw_config_by_version.empty()
      && (override_metadata.raw_config_by_version.crbegin()->first > message_->version());
  }

  config_namespace->num_watchers += message_->overrides().size();

  // If the asked merged config is already deprecated we notify the watcher
  if (notify) {
    spdlog::debug("The document '{}' has been changed", message_->document());
    auto output_message = message_->make_output_message();
    output_message->set_uid(message_->uid());

    auto api_get_command = std::make_shared<scheduler::command::ApiGetCommand>(
      std::make_shared<::mhconfig::api::stream::WatchGetRequest>(
        message_,
        output_message
      )
    );
    scheduler_queue.push(api_get_command);
  }

  return NamespaceExecutionResult::OK;
}

bool ApiWatchCommand::on_get_namespace_error(
  Queue<worker::command::CommandRef>& worker_queue
) {
  //TODO
  return false;
}

} /* command */
} /* scheduler */
} /* mhconfig */
