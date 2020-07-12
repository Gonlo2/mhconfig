#include "mhconfig/scheduler/api_watch_command.h"

namespace mhconfig
{
namespace scheduler
{

ApiWatchCommand::ApiWatchCommand(
  std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message
) : SchedulerCommand(),
    message_(message)
{
}

ApiWatchCommand::~ApiWatchCommand() {
}

std::string ApiWatchCommand::name() const {
  return "API_WATCH";
}

SchedulerCommand::CommandType ApiWatchCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiWatchCommand::namespace_path() const {
  return message_->root_path();
}

SchedulerCommand::CommandResult ApiWatchCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  if (!validate_request(config_namespace, worker_queue)) {
    return CommandResult::OK;
  }

  bool notify = false;

  for_each_document_override_path(
    message_->flavors(),
    message_->overrides(),
    message_->document(),
    [&config_namespace, &notify, this](const auto& override_path) {
      auto& override_metadata = config_namespace.override_metadata_by_override_path[override_path];
      //TODO Check if the overrides are distinct
      override_metadata.watchers.push_back(message_);
      notify |= !override_metadata.raw_config_by_version.empty()
        && (override_metadata.raw_config_by_version.crbegin()->first > message_->version());
    }
  );

  // TODO This will trigger also a update if any of the overrides templates change
  // although only the last one is used, review if is neccesary deal with multiples
  // templates or this is a silly use case.
  if (!message_->template_().empty()) {
    std::string override_path;
    for (const auto& override_: message_->overrides()) {
      make_override_path(
        override_,
        message_->template_(),
        "",
        override_path
      );
      auto& override_metadata = config_namespace.override_metadata_by_override_path[override_path];
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
      std::make_unique<scheduler::ApiGetCommand>(
        std::make_shared<::mhconfig::api::stream::WatchGetRequest>(
          message_,
          output_message
        )
      )
    );
  }

  return CommandResult::OK;
}

bool ApiWatchCommand::validate_request(
  const config_namespace_t& config_namespace,
  WorkerQueue& worker_queue
) {
  bool ok = builder::are_valid_arguments(
    message_->overrides(),
    message_->flavors(),
    message_->document(),
    message_->template_()
  );
  if (!ok) {
    auto output_message = message_->make_output_message();
    output_message->set_status(api::stream::WatchStatus::ERROR);

    worker_queue.push(
      std::make_unique<worker::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return false;
  }

  if (config_namespace.current_version < message_->version()) {
    auto output_message = message_->make_output_message();
    output_message->set_status(api::stream::WatchStatus::INVALID_VERSION);

    worker_queue.push(
      std::make_unique<worker::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return false;
  }

  return true;
}

bool ApiWatchCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  message_->unregister();

  auto output_message = message_->make_output_message();
  output_message->set_status(::mhconfig::api::stream::WatchStatus::ERROR);

  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(output_message)
    )
  );

  return true;
}

} /* scheduler */
} /* mhconfig */
