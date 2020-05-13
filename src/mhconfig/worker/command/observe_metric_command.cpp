#include "mhconfig/worker/command/observe_metric_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ObserveMetricCommand::ObserveMetricCommand(
  metrics::MetricsService::ObservableId id,
  std::map<std::string, std::string>&& labels,
  double value
)
  : Command(),
  id_(id),
  labels_(std::move(labels)),
  value_(value)
{
}

ObserveMetricCommand::~ObserveMetricCommand() {
}

std::string ObserveMetricCommand::name() const {
  return "OBSERVE_METRIC";
}

bool ObserveMetricCommand::execute(
  context_t& context
) {
  context.metrics_service.observe(
    id_,
    std::move(labels_),
    value_
  );
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
