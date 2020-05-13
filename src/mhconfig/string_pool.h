#ifndef MHCONFIG__STRING_POOL_H
#define MHCONFIG__STRING_POOL_H

#include "string_pool/pool.h"
#include "mhconfig/metrics/metrics_service.h"

namespace mhconfig
{
namespace string_pool
{

class MetricsStatsObserver : public ::string_pool::StatsObserver
{
public:
  MetricsStatsObserver(
    metrics::MetricsService& metrics,
    const std::string& id
  ) : metrics_(metrics),
    id_(id)
  {
  }

  virtual ~MetricsStatsObserver() {
  }

  void on_updated_stats(const ::string_pool::stats_t& stats) override {
    metrics_.set(
      metrics::MetricsService::GaugeId::STRING_POOL_NUM_STRINGS,
      {{"pool", id_}},
      stats.num_strings
    );

    metrics_.set(
      metrics::MetricsService::GaugeId::STRING_POOL_NUM_CHUNKS,
      {{"pool", id_}},
      stats.num_chunks
    );

    metrics_.set(
      metrics::MetricsService::GaugeId::STRING_POOL_RECLAIMED_BYTES,
      {{"pool", id_}},
      stats.reclaimed_bytes
    );

    metrics_.set(
      metrics::MetricsService::GaugeId::STRING_POOL_USED_BYTES,
      {{"pool", id_}},
      stats.used_bytes
    );
  }

private:
  metrics::MetricsService& metrics_;
  std::string id_;
};

} /* string_pool */
} /* mhconfig */

#endif
