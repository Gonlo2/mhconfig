#ifndef MHCONFIG__STRING_POOL_H
#define MHCONFIG__STRING_POOL_H

#include "jmutils/string/pool.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{
namespace string_pool
{

class MetricsStatsObserver final : public jmutils::string::StatsObserver
{
public:
  MetricsStatsObserver(
    Metrics& metrics,
    const std::string& id
  ) : metrics_(metrics),
    id_(id),
    sequential_id_(0)
  {
  }

  void on_updated_stats(const jmutils::string::stats_t& stats, bool force) override {
    if (force || (sequential_id_++ == 0)) {
      metrics_.add(
        Metrics::Id::STRING_POOL_NUM_STRINGS,
        {{"pool", id_}},
        stats.num_strings
      );

      metrics_.add(
        Metrics::Id::STRING_POOL_NUM_CHUNKS,
        {{"pool", id_}},
        stats.num_chunks
      );

      metrics_.add(
        Metrics::Id::STRING_POOL_RECLAIMED_BYTES,
        {{"pool", id_}},
        stats.reclaimed_bytes
      );

      metrics_.add(
        Metrics::Id::STRING_POOL_USED_BYTES,
        {{"pool", id_}},
        stats.used_bytes
      );
    }
  }

private:
  Metrics& metrics_;
  std::string id_;
  uint8_t sequential_id_;
};

} /* string_pool */
} /* mhconfig */

#endif
