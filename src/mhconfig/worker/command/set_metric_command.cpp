#include "mhconfig/worker/command/set_metric_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

SetMetricCommand::SetMetricCommand(
  metrics::MetricsService::GaugeId id,
  std::map<std::string, std::string>&& labels,
  double value
)
  : id_(id),
  labels_(std::move(labels)),
  value_(value)
{
}

SetMetricCommand::~SetMetricCommand() {
}

std::string SetMetricCommand::name() const {
  return "SET_METRIC";
}

bool SetMetricCommand::execute(
  context_t& context
) {
  context.metrics_service.set(
    id_,
    std::move(labels_),
    value_
  );
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
