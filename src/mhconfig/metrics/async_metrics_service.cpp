#include "mhconfig/metrics/async_metrics_service.h"

namespace mhconfig
{
namespace metrics
{

  AsyncMetricsService::AsyncMetricsService(
    std::shared_ptr<MetricsQueue::Sender> sender
  )
    : sender_(sender)
  {}

  AsyncMetricsService::~AsyncMetricsService() {
  }

  void AsyncMetricsService::add(
    MetricId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    sender_->push(
      std::make_unique<AddMetric>(id, std::move(labels), value)
    );
  }

  // TODO
  void AsyncMetricsService::clear(
    MetricId id
  ) {
  }

  void AsyncMetricsService::set_namespaces_metrics(
    std::vector<namespace_metrics_t>&& namespaces_metrics
  ) {
    sender_->push(
      std::make_unique<SetNamespacesMetrics>(std::move(namespaces_metrics))
    );
  }

} /* metrics */
} /* mhconfig */
