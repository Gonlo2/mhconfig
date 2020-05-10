#include "mhconfig/worker/command/optimize_merged_config_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

OptimizeMergedConfigCommand::OptimizeMergedConfigCommand(
  std::shared_ptr<merged_config_t> merged_config,
  std::shared_ptr<string_pool::Pool> pool
)
  : Command(),
  merged_config_(merged_config),
  pool_(pool)
{
}

OptimizeMergedConfigCommand::~OptimizeMergedConfigCommand() {
}

std::string OptimizeMergedConfigCommand::name() const {
  return "OPTIMIZE_MERGED_CONFIG";
}

bool OptimizeMergedConfigCommand::force_take_metric() const {
  return true;
}

bool OptimizeMergedConfigCommand::execute(
  context_t& context
) {
  auto optimized_merged_config = std::make_shared<mhconfig::api::config::OptimizedMergedConfig>();
  bool ok = optimized_merged_config->init(
    merged_config_->value,
    pool_
  );
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
