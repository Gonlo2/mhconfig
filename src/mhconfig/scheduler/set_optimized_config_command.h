#ifndef MHCONFIG__SCHEDULER__SET_OPTIMIZED_CONFIG_COMMAND_H
#define MHCONFIG__SCHEDULER__SET_OPTIMIZED_CONFIG_COMMAND_H

#include <memory>

#include "mhconfig/command.h"
#include "mhconfig/config_namespace.h"

namespace mhconfig
{
namespace scheduler
{

class SetOptimizedConfigCommand : public SchedulerCommand
{
public:
  SetOptimizedConfigCommand(
    std::shared_ptr<merged_config_t>&& merged_config
  );

  virtual ~SetOptimizedConfigCommand();

  std::string name() const override;

  CommandType type() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::shared_ptr<merged_config_t> merged_config_;
};

} /* scheduler */
} /* mhconfig */

#endif
