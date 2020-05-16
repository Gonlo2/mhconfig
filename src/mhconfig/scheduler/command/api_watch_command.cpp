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
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  // First we check if the asked version is lower that the current one
  if (config_namespace.current_version < message_->version()) {
    auto output_message = message_->make_output_message();
    output_message->set_uid(message_->uid());
    output_message->set_status(::mhconfig::api::stream::watch::Status::INVALID_VERSION);

    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return NamespaceExecutionResult::OK;
  }

  bool notify = false;

  auto document_metadata = config_namespace
    .document_metadata_by_document[message_->document()];

  for (const auto& override_: message_->overrides()) {
    auto& override_metadata = document_metadata->override_by_key[override_];
    //TODO Check if the overrides are distinct
    override_metadata.watchers.emplace_back(message_);
    notify |= !override_metadata.raw_config_by_version.empty()
      && (override_metadata.raw_config_by_version.crbegin()->first > message_->version());
  }

  config_namespace.num_watchers += message_->overrides().size();

  // If the asked merged config is already deprecated we notify the watcher
  if (notify) {
    spdlog::debug("The document '{}' has been changed", message_->document());
    auto output_message = message_->make_output_message();
    output_message->set_uid(message_->uid());

    scheduler_queue.push(
      std::make_unique<scheduler::command::ApiGetCommand>(
        std::make_shared<::mhconfig::api::stream::WatchGetRequest>(
          message_,
          output_message
        )
      )
    );
  }

  return NamespaceExecutionResult::OK;
}

bool ApiWatchCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  auto output_message = message_->make_output_message();
  output_message->set_uid(message_->uid());
  output_message->set_status(::mhconfig::api::stream::watch::Status::ERROR);

  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(output_message)
    )
  );

  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
