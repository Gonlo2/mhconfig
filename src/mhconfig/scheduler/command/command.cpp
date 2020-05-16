#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{

namespace scheduler
{
namespace command
{

Command::Command() {
}

Command::~Command() {
}

const std::string& Command::namespace_path() const {
  return EMPTY_STRING;
}

uint64_t Command::namespace_id() const {
  return 0;
}

const std::shared_ptr<config_namespace_t> Command::config_namespace() const {
  return nullptr;
}

NamespaceExecutionResult Command::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  return NamespaceExecutionResult::ERROR;
}

bool Command::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  return false;
}

bool Command::execute(
  scheduler_context_t& context
) {
  return false;
}

} /* command */
} /* scheduler */


namespace worker
{
namespace command
{

Command::Command() {
}
Command::~Command() {
}

bool Command::force_take_metric() const {
  return false;
}

} /* command */
} /* worker */

} /* mhconfig */
