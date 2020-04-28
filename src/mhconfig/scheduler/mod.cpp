#include "mhconfig/scheduler/mod.h"

namespace mhconfig
{
namespace scheduler
{

Scheduler::Scheduler(
  Queue<command::CommandRef>& scheduler_queue,
  Queue<mhconfig::worker::command::CommandRef>& worker_queue,
  Metrics& metrics
) :
  jmutils::parallelism::Worker<Scheduler, command::CommandRef>(scheduler_queue, 1),
  worker_queue_(worker_queue),
  metrics_(metrics)
{
}

Scheduler::~Scheduler() {
}

std::pair<Scheduler::ConfigNamespaceState, std::shared_ptr<config_namespace_t>> Scheduler::get_or_build_namespace(
  command::CommandRef command
) {
  // First we search for the namespace
  auto search = namespace_by_path_.find(command->namespace_path());
  if (search == namespace_by_path_.end()) {
    // If it isn't present we check if some another command ask for it
    auto search_commands_waiting = commands_waiting_for_namespace_by_path_.find(
      command->namespace_path()
    );

    if (search_commands_waiting == commands_waiting_for_namespace_by_path_.end()) {
      auto setup_command = std::make_shared<::mhconfig::worker::command::SetupCommand>(
        command->namespace_path()
      );
      worker_queue_.push(setup_command);

      commands_waiting_for_namespace_by_path_[command->namespace_path()].push_back(command);
    } else {
      // In other case we wait for the namespace
      search_commands_waiting->second.push_back(command);
    }

    // In this case we need to wait
    return std::make_pair(ConfigNamespaceState::BUILDING, nullptr);
  }

  // If some namespace is present we check if it's well formed
  if (search->second == nullptr || !search->second->ok) {
    return std::make_pair(ConfigNamespaceState::ERROR, nullptr);
  }

  // If we are here then we have the namespace and before return it
  // we update the last access timestamp
  search->second->last_access_timestamp = jmutils::time::monotonic_now_sec();

  return std::make_pair(ConfigNamespaceState::OK, search->second);
}

} /* scheduler */
} /* mhconfig */
