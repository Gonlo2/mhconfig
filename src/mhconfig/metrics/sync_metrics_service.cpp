#include "mhconfig/metrics/sync_metrics_service.h"

namespace mhconfig
{
namespace metrics
{

  SyncMetricsService::SyncMetricsService(
    const std::string& address
  )
    : MetricsService(),
    exposer_(address)
  {}

  SyncMetricsService::~SyncMetricsService() {
  }

  void SyncMetricsService::init() {
    registry_ = std::make_shared<prometheus::Registry>();;
    exposer_.RegisterCollectable(registry_);

    family_api_duration_summary_ = &prometheus::BuildSummary()
      .Name("api_duration_nanoseconds")
      .Help("How many nanoseconds takes each API request")
      .Register(*registry_);

    family_scheduler_duration_summary_ = &prometheus::BuildSummary()
      .Name("scheduler_duration_nanoseconds")
      .Help("How many nanoseconds takes process a task by the scheduler thread")
      .Register(*registry_);

    family_worker_duration_summary_ = &prometheus::BuildSummary()
      .Name("worker_duration_nanoseconds")
      .Help("How many nanoseconds takes process a task by a worker thread")
      .Register(*registry_);
  }

  void SyncMetricsService::observe(
    MetricId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    switch (id) {
      case MetricId::API_DURATION_NANOSECONDS:
        family_api_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case MetricId::SCHEDULER_DURATION_NANOSECONDS:
        family_scheduler_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case MetricId::WORKER_DURATION_NANOSECONDS:
        family_worker_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
    }

    spdlog::warn("Unknown metric id {}", id);
  }

} /* metrics */
} /* mhconfig */
