#include "mhconfig/scheduler/add_namespace_command.h"

namespace mhconfig
{
namespace scheduler
{

AddNamespaceCommand::AddNamespaceCommand(
  std::shared_ptr<config_namespace_t> config_namespace
) : SchedulerCommand(),
    config_namespace_(config_namespace)
{
}

AddNamespaceCommand::~AddNamespaceCommand() {
}

std::string AddNamespaceCommand::name() const {
  return "ADD_NAMESPACE";
}

SchedulerCommand::CommandType AddNamespaceCommand::type() const {
  return CommandType::ADD_NAMESPACE;
}

const std::string& AddNamespaceCommand::namespace_path() const {
  return config_namespace_->root_path;
}

uint64_t AddNamespaceCommand::namespace_id() const {
  return config_namespace_->id;
}

const std::shared_ptr<config_namespace_t> AddNamespaceCommand::config_namespace() const {
  return config_namespace_;
}

} /* scheduler */
} /* mhconfig */
