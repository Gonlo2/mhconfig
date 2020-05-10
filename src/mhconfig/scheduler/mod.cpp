#include "mhconfig/scheduler/mod.h"

namespace mhconfig
{
namespace scheduler
{

Scheduler::Scheduler(
  Queue<command::CommandRef>& scheduler_queue,
  Queue<mhconfig::worker::command::CommandRef>& worker_queue,
  metrics::MetricsService& metrics
) :
  jmutils::parallelism::Worker<Scheduler, command::CommandRef>(scheduler_queue, 1),
  context_(worker_queue, metrics)
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
  }
}

} /* scheduler */
} /* mhconfig */
