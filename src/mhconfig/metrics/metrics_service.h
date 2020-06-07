#ifndef MHCONFIG__METRICS__METRICS_SERVICE_H
#define MHCONFIG__METRICS__METRICS_SERVICE_H

#include <map>
#include <string>
#include <vector>
#include <absl/container/flat_hash_map.h>
#include "jmutils/common.h"
#include "mhconfig/api/stream/watch_stream.h"

namespace mhconfig
{
namespace metrics
{

  class MetricsService
  {
  public:
    enum MetricId {
      // Observable
      API_DURATION_NANOSECONDS,
      SCHEDULER_DURATION_NANOSECONDS,
      WORKER_DURATION_NANOSECONDS,
      OPTIMIZED_MERGED_CONFIG_USED_BYTES,

      // Gauges
      STRING_POOL_NUM_STRINGS,
      STRING_POOL_NUM_CHUNKS,
      STRING_POOL_RECLAIMED_BYTES,
      STRING_POOL_USED_BYTES,
      ASKED_CONFIGS,
      REGISTERED_WATCHERS
    };

    struct namespace_metrics_t {
      std::string name;
      absl::flat_hash_map<std::pair<std::string, std::string>, ::jmutils::zero_value_t<uint32_t>> asked_configs;
      std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers;
    };

    MetricsService();
    virtual ~MetricsService();

    virtual void add(
      MetricId id,
      std::map<std::string, std::string>&& labels,
      double value
    ) = 0;

    virtual void clear(
      MetricId id
    ) = 0;

    virtual void set_namespaces_metrics(
      std::vector<namespace_metrics_t>&& namespaces_metrics
    ) = 0;

  };

} /* metrics */
} /* mhconfig */

#endif
