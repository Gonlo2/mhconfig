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

    void observe(
      ObservableId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) override;

    void set(
      GaugeId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) override;

  private:
    std::shared_ptr<MetricsQueue::Sender> sender_;
  };

} /* metrics */
} /* mhconfig */

#endif
