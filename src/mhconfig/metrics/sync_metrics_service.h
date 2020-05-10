#ifndef MHCONFIG__METRICS__SYNC_METRICS_SERVICE_H
#define MHCONFIG__METRICS__SYNC_METRICS_SERVICE_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/async.h"

#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/worker/command/observe_metric_command.h"

namespace mhconfig
{
namespace metrics
{

  class SyncMetricsService : public MetricsService
  {
  public:
    SyncMetricsService(const std::string& address);
    virtual ~SyncMetricsService();

    void init();

    void observe(
      MetricId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) override;

  private:
    prometheus::Exposer exposer_;
    std::shared_ptr<prometheus::Registry> registry_;

    prometheus::Summary::Quantiles quantiles_ = {
      {0.5, 0.05},
      {0.90, 0.01},
      {0.99, 0.001}
    };

    prometheus::Family<prometheus::Summary>* family_api_duration_summary_;
    prometheus::Family<prometheus::Summary>* family_scheduler_duration_summary_;
    prometheus::Family<prometheus::Summary>* family_worker_duration_summary_;
    prometheus::Family<prometheus::Summary>* family_serialization_duration_summary_;
  };

} /* metrics */
} /* mhconfig */

#endif
