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
  auto config_namespace = mhconfig::builder::index_files(
    path_,
    context.metrics_service
  );
  context.scheduler_queue->push(
    std::make_unique<scheduler::command::AddNamespaceCommand>(config_namespace)
  );
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
