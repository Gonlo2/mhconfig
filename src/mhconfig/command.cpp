#include "mhconfig/command.h"

namespace mhconfig
{

SchedulerCommand::~SchedulerCommand() {
}

const std::string& SchedulerCommand::namespace_path() const {
  return EMPTY_STRING;
}

uint64_t SchedulerCommand::namespace_id() const {
  return 0;
}

const std::shared_ptr<config_namespace_t> SchedulerCommand::config_namespace() const {
  return nullptr;
}

SchedulerCommand::CommandResult SchedulerCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  return CommandResult::ERROR;
}

bool SchedulerCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  return false;
}

bool SchedulerCommand::execute(
  context_t& context
) {
  return false;
}


WorkerCommand::~WorkerCommand() {
}

bool WorkerCommand::force_take_metric() const {
  return false;
}

} /* mhconfig */
