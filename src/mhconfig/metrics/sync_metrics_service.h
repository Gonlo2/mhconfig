#ifndef MHCONFIG__METRICS__SYNC_METRICS_SERVICE_H
#define MHCONFIG__METRICS__SYNC_METRICS_SERVICE_H

#include "spdlog/spdlog.h"

#include <prometheus/summary.h>
#include <prometheus/gauge.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "mhconfig/metrics/metrics_service.h"

namespace mhconfig
{
namespace metrics
{

  class SyncMetricsService : public MetricsService
  {
  public:
    explicit SyncMetricsService(const std::string& address);
    virtual ~SyncMetricsService();

    void init();

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
    prometheus::Exposer exposer_;
    std::shared_ptr<prometheus::Registry> registry_;

    prometheus::Summary::Quantiles quantiles_ = {
      {0.5, 0.05},
      {0.90, 0.01},
      {0.99, 0.001}
    };

    prometheus::Family<prometheus::Summary>* family_api_duration_summary_{nullptr};
    prometheus::Family<prometheus::Summary>* family_scheduler_duration_summary_{nullptr};
    prometheus::Family<prometheus::Summary>* family_worker_duration_summary_{nullptr};
    prometheus::Family<prometheus::Summary>* family_serialization_duration_summary_{nullptr};
    prometheus::Family<prometheus::Summary>* family_optimized_merged_config_used_bytes_summary_{nullptr};

    prometheus::Family<prometheus::Gauge>* family_string_pool_num_strings_gauge_{nullptr};
    prometheus::Family<prometheus::Gauge>* family_string_pool_num_chunks_gauge_{nullptr};
    prometheus::Family<prometheus::Gauge>* family_string_pool_reclaimed_bytes_gauge_{nullptr};
    prometheus::Family<prometheus::Gauge>* family_string_pool_used_bytes_gauge_{nullptr};
    prometheus::Family<prometheus::Gauge>* family_asked_configs_gauge_{nullptr};
    prometheus::Family<prometheus::Gauge>* family_registered_watchers_gauge_{nullptr};
  };

} /* metrics */
} /* mhconfig */

#endif
