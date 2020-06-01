#ifndef MHCONFIG__SCHEDULER__COMMAND__SET_OPTIMIZED_CONFIG_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__SET_OPTIMIZED_CONFIG_COMMAND_H

#include <memory>

#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/ds/config_namespace.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace mhconfig::ds::config_namespace;

class SetOptimizedConfigCommand : public Command
{
public:
  SetOptimizedConfigCommand(
    std::shared_ptr<mhconfig::ds::config_namespace::merged_config_t>&& merged_config
  );

  virtual ~SetOptimizedConfigCommand();

  std::string name() const override;

  CommandType command_type() const override;

  bool execute(
    scheduler_context_t& context
  ) override;

private:
  std::shared_ptr<mhconfig::ds::config_namespace::merged_config_t> merged_config_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
