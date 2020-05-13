#ifndef MHCONFIG__WORKER__COMMAND__SET_METRIC_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__SET_METRIC_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/worker/command/command.h"
#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

class SetMetricCommand : public Command
{
public:
  SetMetricCommand(
    metrics::MetricsService::GaugeId id,
    std::map<std::string, std::string>&& labels,
    double value
  );
  virtual ~SetMetricCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  metrics::MetricsService::GaugeId id_;
  std::map<std::string, std::string> labels_;
  double value_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
