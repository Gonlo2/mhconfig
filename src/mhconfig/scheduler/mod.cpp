#include "mhconfig/scheduler/mod.h"

namespace mhconfig
{
namespace scheduler
{

Scheduler::Scheduler(
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue,
  std::unique_ptr<metrics::MetricsService>&& metrics
)
  : scheduler_queue_(scheduler_queue),
  context_(worker_queue, std::move(metrics))
{
}

Scheduler::~Scheduler() {
}

void Scheduler::softdelete_namespace(
  config_namespace_t& config_namespace
) {
  auto search = context_.namespace_by_path.find(config_namespace.root_path);
  if (
    (search != context_.namespace_by_path.end())
    && (search->second->id == config_namespace.id)
  ) {
    spdlog::info(
      "Doing a softdelete of the namespace '{}'",
      config_namespace.root_path
    );
    context_.namespace_by_path.erase(search);

    if (!config_namespace.watchers.empty()) {
      decltype(config_namespace.watchers) watchers_to_remove;
      std::swap(config_namespace.watchers, watchers_to_remove);

      spdlog::debug("To unregister {} watchers", watchers_to_remove.size());

      context_.worker_queue.push(
        std::make_unique<::mhconfig::worker::command::UnregisterWatchersCommand>(
          std::move(watchers_to_remove)
        )
      );
    }
  }
}

} /* scheduler */
} /* mhconfig */
