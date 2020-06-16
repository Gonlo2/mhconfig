#include "mhconfig/scheduler/command/set_optimized_config_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

SetOptimizedConfigCommand::SetOptimizedConfigCommand(
  std::shared_ptr<mhconfig::ds::config_namespace::merged_config_t>&& merged_config
)
  : merged_config_(std::move(merged_config))
{
}

SetOptimizedConfigCommand::~SetOptimizedConfigCommand() {
}

std::string SetOptimizedConfigCommand::name() const {
  return "SET_OPTIMIZED_CONFIG";
}

CommandType SetOptimizedConfigCommand::command_type() const {
  return CommandType::GENERIC;
}

bool SetOptimizedConfigCommand::execute(
  scheduler_context_t& context
) {
  // The most silly command ever existed
  merged_config_->status = MergedConfigStatus::OK_CONFIG_OPTIMIZED;
  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
