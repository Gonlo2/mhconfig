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

    if (config_namespace.num_watchers) {
      std::unordered_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers_to_remove;

      for (auto& document_metadata_it: config_namespace.document_metadata_by_document) {
        for (auto& override_metadata_it : document_metadata_it.second->override_by_key) {
          auto& watchers = override_metadata_it.second.watchers;
          for (auto& weak_ptr : watchers) {
            if (auto watcher = weak_ptr.lock()) {
              watchers_to_remove.insert(watcher);
            }
          }
          watchers.clear();
        }
      }

      spdlog::debug("To unregister {} watchers", watchers_to_remove.size());
      config_namespace.num_watchers = 0;

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
