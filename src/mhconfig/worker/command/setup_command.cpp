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

bool SetupCommand::execute(
  Queue<scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) {
  auto config_namespace = mhconfig::builder::index_files(path_);
  auto add_namespace_command = std::make_shared<scheduler::command::AddNamespaceCommand>(config_namespace);
  scheduler_queue.push(add_namespace_command);
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
