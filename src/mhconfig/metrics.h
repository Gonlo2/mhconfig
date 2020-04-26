#ifndef MHCONFIG__METRICS_H
#define MHCONFIG__METRICS_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/async.h"

#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace mhconfig
{
  class Metrics
  {
  public:
    Metrics(const std::string& address);
    virtual ~Metrics();

    void init();

    void api_duration(const std::string& type, double nanoseconds);
    void scheduler_duration(const std::string& type, double duration_ns);
    void serialization_duration(double duration_ns);

  private:
    prometheus::Exposer exposer_;
    std::shared_ptr<prometheus::Registry> registry_;

    prometheus::Summary::Quantiles quantiles_ = {{0.5, 0.05}, {0.90, 0.01}, {0.99, 0.001}};

    prometheus::Family<prometheus::Summary>* family_api_duration_summary_;
    prometheus::Family<prometheus::Summary>* family_scheduler_duration_summary_;
    prometheus::Family<prometheus::Summary>* family_serialization_duration_summary_;

  };
} /* mhconfig */

#endif /* ifndef MHCONFIG__MHCONFIG_H */
