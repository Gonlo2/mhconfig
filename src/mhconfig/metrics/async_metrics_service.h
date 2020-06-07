#ifndef MHCONFIG__METRICS__ASYNC_METRICS_SERVICE_H
#define MHCONFIG__METRICS__ASYNC_METRICS_SERVICE_H

#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/metrics/metrics_worker.h"

namespace mhconfig
{
namespace metrics
{
  class AsyncMetricsService : public MetricsService
  {
  public:
    explicit AsyncMetricsService(
      std::shared_ptr<MetricsQueue::Sender> sender
    );
    virtual ~AsyncMetricsService();

    void add(
      MetricId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) override;

    void clear(
      MetricId id
    ) override;

    void set_namespaces_metrics(
      std::vector<namespace_metrics_t>&& namespaces_metrics
    ) override;

  private:
    std::shared_ptr<MetricsQueue::Sender> sender_;
  };

} /* metrics */
} /* mhconfig */

#endif
