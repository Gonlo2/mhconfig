#ifndef MHCONFIG__SCHEDULER__COMMAND__OBTAIN_USAGE_METRICS_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__OBTAIN_USAGE_METRICS_COMMAND_H

#include <memory>

#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

class ObtainUsageMetricsCommand : public Command
{
public:
  ObtainUsageMetricsCommand();
  virtual ~ObtainUsageMetricsCommand();

  std::string name() const override;

  CommandType command_type() const override;

  bool execute(
    scheduler_context_t& context
  ) override;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
