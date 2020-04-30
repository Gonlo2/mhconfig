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

void Scheduler::softdelete_namespace(
  std::shared_ptr<config_namespace_t> config_namespace
) {
  auto search = namespace_by_path_.find(config_namespace->root_path);
  if (
    (search != namespace_by_path_.end())
    && (search->second->id == config_namespace->id)
  ) {
    spdlog::info(
      "Doing a softdelete of the namespace '{}'",
      config_namespace->root_path
    );
    namespace_by_path_.erase(search);
  }
}

} /* scheduler */
} /* mhconfig */
