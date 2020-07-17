#include "mhconfig/scheduler/on_watchers_removed_command.h"

namespace mhconfig
{
namespace scheduler
{

OnWatchersRemovedCommand::OnWatchersRemovedCommand(
  std::shared_ptr<api::stream::WatchInputMessage>&& watcher
) : root_path_(watcher->root_path())
{
  watchers_.push_back(watcher);
}

OnWatchersRemovedCommand::OnWatchersRemovedCommand(
  const std::string& root_path,
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>>&& watchers
) : root_path_(root_path),
  watchers_(std::move(watchers))
{
}

OnWatchersRemovedCommand::~OnWatchersRemovedCommand() {
}

std::string OnWatchersRemovedCommand::name() const {
  return "ON_WATCHERS_REMOVED";
}

SchedulerCommand::CommandType OnWatchersRemovedCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& OnWatchersRemovedCommand::namespace_path() const {
  return root_path_;
}

SchedulerCommand::CommandResult OnWatchersRemovedCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  std::vector<std::shared_ptr<api::Commitable>> traces;

  for (size_t i = 0, l = watchers_.size(); i < l; ++i) {
    for_each_trace_to_trigger(
      config_namespace,
      watchers_[i].get(),
      [&traces](auto namespace_id, const auto* message, auto* trace) {
        traces.push_back(
          scheduler::make_trace_output_message(
            trace,
            api::stream::TraceOutputMessage::Status::REMOVED_WATCHER,
            namespace_id,
            message->version(),
            message
          )
        );
      }
    );
  }

  if (!traces.empty()) {
    worker_queue.push(
      std::make_unique<worker::ApiBatchReplyCommand>(
        std::move(traces)
      )
    );
  }

  return CommandResult::OK;
}

bool OnWatchersRemovedCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  return true;
}

} /* scheduler */
} /* mhconfig */
