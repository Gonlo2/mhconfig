#include "mhconfig/worker/command/optimize_merged_config_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

OptimizeMergedConfigCommand::OptimizeMergedConfigCommand(
  std::shared_ptr<merged_config_t> merged_config
)
  : Command(),
  merged_config_(merged_config)
{
}

OptimizeMergedConfigCommand::~OptimizeMergedConfigCommand() {
}

std::string OptimizeMergedConfigCommand::name() const {
  return "OPTIMIZE_MERGED_CONFIG";
}

bool OptimizeMergedConfigCommand::execute(
  Queue<scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) {
  auto optimized_merged_config = std::make_shared<mhconfig::api::config::OptimizedMergedConfig>();
  bool ok = optimized_merged_config->init(merged_config_->value);
  if (ok) {
    // This works only because the the api merged config object is modified only in the
    // scheduler thread and that modification trigger this command, so at this point
    // this thread is the only place where this variable is modified.
    // If some other MergedConfig strategy is made THIS SHOULD BE CHANGED TO SWAP IT IN
    // THE SCHEDULER THREAD
    merged_config_->api_merged_config = optimized_merged_config;
  } else {
    spdlog::warn("Can't optimize the config of the document");
  }
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */