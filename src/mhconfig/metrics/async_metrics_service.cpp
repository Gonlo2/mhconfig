#include "mhconfig/metrics/async_metrics_service.h"

namespace mhconfig
{
namespace metrics
{

  AsyncMetricsService::AsyncMetricsService(
    Queue<worker::command::CommandRef>& worker_queue
  )
    : MetricsService(),
    worker_queue_(worker_queue)
  {}

  AsyncMetricsService::~AsyncMetricsService() {
  }

  void AsyncMetricsService::observe(
    MetricId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    auto observe_metric_command = std::make_shared<::mhconfig::worker::command::ObserveMetricCommand>(
      id,
      std::move(labels),
      value
    );
    worker_queue_.push(observe_metric_command);
  }

} /* metrics */
} /* mhconfig */
