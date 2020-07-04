#ifndef MHCONFIG__SCHEDULER__OBTAIN_USAGE_METRICS_COMMAND_H
#define MHCONFIG__SCHEDULER__OBTAIN_USAGE_METRICS_COMMAND_H

#include <memory>

#include "mhconfig/command.h"

namespace mhconfig
{
namespace scheduler
{

class ObtainUsageMetricsCommand : public SchedulerCommand
{
public:
  ObtainUsageMetricsCommand();
  virtual ~ObtainUsageMetricsCommand();

  std::string name() const override;

  CommandType type() const override;

  bool execute(
    context_t& context
  ) override;
};

} /* scheduler */
} /* mhconfig */

#endif
