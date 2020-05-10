#ifndef MHCONFIG__WORKER__COMMAND__OBSERVE_METRIC_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__OBSERVE_METRIC_COMMAND_H

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

class ObserveMetricCommand : public Command
{
public:
  ObserveMetricCommand(
    metrics::MetricsService::MetricId id,
    std::map<std::string, std::string>&& labels,
    double value
  );
  virtual ~ObserveMetricCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  metrics::MetricsService::MetricId id_;
  std::map<std::string, std::string> labels_;
  double value_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
