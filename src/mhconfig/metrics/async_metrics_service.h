#ifndef MHCONFIG__METRICS__ASYNC_METRICS_SERVICE_H
#define MHCONFIG__METRICS__ASYNC_METRICS_SERVICE_H

#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/worker/command/observe_metric_command.h"

namespace mhconfig
{
namespace metrics
{
  using jmutils::container::Queue;

  class AsyncMetricsService : public MetricsService
  {
  public:
    AsyncMetricsService(
      Queue<worker::command::CommandRef>& worker_queue
    );
    virtual ~AsyncMetricsService();

    void observe(
      MetricsService::MetricId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) override;

  private:
    Queue<worker::command::CommandRef>& worker_queue_;
  };

} /* metrics */
} /* mhconfig */

#endif
