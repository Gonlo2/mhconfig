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
    output_message->set_status(::mhconfig::api::stream::WatchStatus::INVALID_VERSION);

    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return NamespaceExecutionResult::OK;
  }

  if (message_->document().empty() || (message_->document()[0] == '_')) {
    auto output_message = message_->make_output_message();
    output_message->set_status(::mhconfig::api::stream::WatchStatus::ERROR);

    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return NamespaceExecutionResult::OK;
  }

  bool notify = false;

  {
    auto& document_metadata = config_namespace
      .document_metadata_by_document[message_->document()];

    for (const auto& override_: message_->overrides()) {
      auto& override_metadata = document_metadata.override_by_key[override_];
      //TODO Check if the overrides are distinct
      override_metadata.watchers.push_back(message_);
      notify |= !override_metadata.raw_config_by_version.empty()
        && (override_metadata.raw_config_by_version.crbegin()->first > message_->version());
    }
  }

  // TODO This will trigger also a update if any of the overrides templates change
  // although only the last one is used, review if is neccesary deal with multiples
  // templates or this is a silly use case.
  if (!message_->template_().empty()) {
    auto& document_metadata = config_namespace
      .document_metadata_by_document[message_->template_()];

    for (const auto& override_: message_->overrides()) {
      auto& override_metadata = document_metadata.override_by_key[override_];
      //TODO Check if the overrides are distinct
      override_metadata.watchers.push_back(message_);
      notify |= !override_metadata.raw_config_by_version.empty()
        && (override_metadata.raw_config_by_version.crbegin()->first > message_->version());
    }
  }

  config_namespace.watchers.push_back(message_);

  // If the asked merged config is already deprecated we notify the watcher
  if (notify) {
    spdlog::debug("The document '{}' has been changed", message_->document());
    auto output_message = message_->make_output_message();

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
  message_->unregister();

  auto output_message = message_->make_output_message();
  output_message->set_status(::mhconfig::api::stream::WatchStatus::ERROR);

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
