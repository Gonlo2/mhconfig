#include "mhconfig/worker/command/setup_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

SetupCommand::SetupCommand(const std::string& path)
  : Command(), path_(path)
{
}

SetupCommand::~SetupCommand() {
}

std::string SetupCommand::name() const {
  return "SETUP";
}

bool SetupCommand::force_take_metric() const {
  return true;
}

bool SetupCommand::execute(
  context_t& context
) {
  auto config_namespace = mhconfig::builder::index_files(path_);
  auto add_namespace_command = std::make_shared<scheduler::command::AddNamespaceCommand>(config_namespace);
  context.scheduler_queue.push(add_namespace_command);
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
