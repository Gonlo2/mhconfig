#include "mhconfig/scheduler/set_optimized_config_command.h"

namespace mhconfig
{
namespace scheduler
{

SetOptimizedConfigCommand::SetOptimizedConfigCommand(
  std::shared_ptr<merged_config_t>&& merged_config
)
  : merged_config_(std::move(merged_config))
{
}

SetOptimizedConfigCommand::~SetOptimizedConfigCommand() {
}

std::string SetOptimizedConfigCommand::name() const {
  return "SET_OPTIMIZED_CONFIG";
}

SchedulerCommand::CommandType SetOptimizedConfigCommand::type() const {
  return CommandType::GENERIC;
}

bool SetOptimizedConfigCommand::execute(
  context_t& context
) {
  // The most silly command ever existed
  merged_config_->status = MergedConfigStatus::OK_CONFIG_OPTIMIZED;
  return true;
}

} /* scheduler */
} /* mhconfig */
