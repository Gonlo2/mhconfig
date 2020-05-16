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
    enum ObservableId {
      API_DURATION_NANOSECONDS,
      SCHEDULER_DURATION_NANOSECONDS,
      WORKER_DURATION_NANOSECONDS,
      OPTIMIZED_MERGED_CONFIG_USED_BYTES
    };

    enum GaugeId {
      STRING_POOL_NUM_STRINGS,
      STRING_POOL_NUM_CHUNKS,
      STRING_POOL_RECLAIMED_BYTES,
      STRING_POOL_USED_BYTES,
    };

    MetricsService();
    virtual ~MetricsService();

    virtual void observe(
      ObservableId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) = 0;

    virtual void set(
      GaugeId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) = 0;
  };

} /* metrics */
} /* mhconfig */

#endif
