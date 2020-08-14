#ifndef MHCONFIG__METRICS__H
#define MHCONFIG__METRICS__H

#include "spdlog/spdlog.h"

#include <prometheus/summary.h>
#include <prometheus/gauge.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace mhconfig
{

class Metrics final
{
public:
  enum class Id {
    // Observable
    API_DURATION_NANOSECONDS,
    WORKER_DURATION_NANOSECONDS,
    OPTIMIZED_MERGED_CONFIG_USED_BYTES,

    // Gauges
    STRING_POOL_NUM_STRINGS,
    STRING_POOL_NUM_CHUNKS,
    STRING_POOL_RECLAIMED_BYTES,
    STRING_POOL_USED_BYTES,
  };

  Metrics() {};

  Metrics(const Metrics&) = delete;
  Metrics(Metrics&&) = delete;

  void init(const std::string& address);

  void add(
    Id id,
    std::map<std::string, std::string>&& labels,
    double value
  );

private:
  std::unique_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;

  prometheus::Summary::Quantiles quantiles_ = {
    {0.5, 0.05},
    {0.90, 0.01},
    {0.99, 0.001}
  };

  prometheus::Family<prometheus::Summary>* family_api_duration_summary_{nullptr};
  prometheus::Family<prometheus::Summary>* family_worker_duration_summary_{nullptr};
  prometheus::Family<prometheus::Summary>* family_serialization_duration_summary_{nullptr};
  prometheus::Family<prometheus::Summary>* family_optimized_merged_config_used_bytes_summary_{nullptr};

  prometheus::Family<prometheus::Gauge>* family_string_pool_num_strings_gauge_{nullptr};
  prometheus::Family<prometheus::Gauge>* family_string_pool_num_chunks_gauge_{nullptr};
  prometheus::Family<prometheus::Gauge>* family_string_pool_reclaimed_bytes_gauge_{nullptr};
  prometheus::Family<prometheus::Gauge>* family_string_pool_used_bytes_gauge_{nullptr};
};

} /* mhconfig */

#endif
