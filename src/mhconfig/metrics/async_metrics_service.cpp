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

  void AsyncMetricsService::observe(
    ObservableId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    sender_->push(
      std::make_unique<ObserveAsyncMetric>(id, std::move(labels), value)
    );
  }

  void AsyncMetricsService::set(
    GaugeId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    sender_->push(
      std::make_unique<SetAsyncMetric>(id, std::move(labels), value)
    );
  }

} /* metrics */
} /* mhconfig */
