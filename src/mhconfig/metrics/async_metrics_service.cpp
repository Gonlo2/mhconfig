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

} /* metrics */
} /* mhconfig */
