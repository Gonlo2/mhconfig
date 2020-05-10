#ifndef MHCONFIG__METRICS__METRICS_SERVICE_H
#define MHCONFIG__METRICS__METRICS_SERVICE_H

#include <map>
#include <string>

namespace mhconfig
{
namespace metrics
{

  class MetricsService
  {
  public:
    enum MetricId {
      API_DURATION_NANOSECONDS,
      SCHEDULER_DURATION_NANOSECONDS,
      WORKER_DURATION_NANOSECONDS,
    };

    MetricsService();
    virtual ~MetricsService();

    virtual void observe(
      MetricId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) = 0;
  };

} /* metrics */
} /* mhconfig */

#endif
