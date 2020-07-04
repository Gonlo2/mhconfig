#include "mhconfig/worker/setup_command.h"

namespace mhconfig
{
namespace worker
{

SetupCommand::SetupCommand(const std::string& path)
  : WorkerCommand(), path_(path)
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
    std::make_unique<scheduler::AddNamespaceCommand>(config_namespace)
  );
  return true;
}

} /* worker */
} /* mhconfig */
