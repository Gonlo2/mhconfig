#include "mhconfig/scheduler/command/obtain_usage_metrics_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ObtainUsageMetricsCommand::ObtainUsageMetricsCommand(
) {
}

ObtainUsageMetricsCommand::~ObtainUsageMetricsCommand() {
}

std::string ObtainUsageMetricsCommand::name() const {
  return "OBTAIN_USAGE_METRICS";
}

CommandType ObtainUsageMetricsCommand::command_type() const {
  return CommandType::GENERIC;
}

bool ObtainUsageMetricsCommand::execute(
  scheduler_context_t& context
) {
  std::vector<::mhconfig::metrics::MetricsService::namespace_metrics_t> namespaces_metrics(
    context.namespace_by_path.size()
  );
  size_t i = 0;
  for (auto& it : context.namespace_by_path) {
    namespaces_metrics[i].name = it.first;
    std::swap(namespaces_metrics[i].asked_configs, it.second->asked_configs);
    it.second->asked_configs.reserve(namespaces_metrics[i].asked_configs.size());
    namespaces_metrics[i].watchers = it.second->watchers;
    ++i;
  }

  context.metrics->set_namespaces_metrics(std::move(namespaces_metrics));

  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
