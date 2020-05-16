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

    family_optimized_merged_config_used_bytes_summary_ = &prometheus::BuildSummary()
      .Name("optimized_merged_config_used_bytes")
      .Help("The number of used bytes in the optimized merged config")
      .Register(*registry_);


    family_string_pool_num_strings_gauge_ = &prometheus::BuildGauge()
      .Name("string_pool_num_strings")
      .Help("The number of strings in the pool")
      .Register(*registry_);

    family_string_pool_num_chunks_gauge_ = &prometheus::BuildGauge()
      .Name("string_pool_num_chunks")
      .Help("The number of chunks in the pool")
      .Register(*registry_);

    family_string_pool_reclaimed_bytes_gauge_ = &prometheus::BuildGauge()
      .Name("string_pool_reclaimed_bytes")
      .Help("The number of reclaimed bytes in the pool")
      .Register(*registry_);

    family_string_pool_used_bytes_gauge_ = &prometheus::BuildGauge()
      .Name("string_pool_used_bytes")
      .Help("The number of used bytes in the pool")
      .Register(*registry_);
  }

  void SyncMetricsService::observe(
    ObservableId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    switch (id) {
      case ObservableId::API_DURATION_NANOSECONDS:
        family_api_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case ObservableId::SCHEDULER_DURATION_NANOSECONDS:
        family_scheduler_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case ObservableId::WORKER_DURATION_NANOSECONDS:
        family_worker_duration_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case ObservableId::OPTIMIZED_MERGED_CONFIG_USED_BYTES:
        family_optimized_merged_config_used_bytes_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
    }

    spdlog::warn("Unknown observable metric id {}", id);
  }

  void SyncMetricsService::set(
    GaugeId id,
    std::map<std::string, std::string>&& labels,
    double value
  ) {
    switch (id) {
      case GaugeId::STRING_POOL_NUM_STRINGS:
        family_string_pool_num_strings_gauge_->Add(labels).Set(value);
        return;
      case GaugeId::STRING_POOL_NUM_CHUNKS:
        family_string_pool_num_chunks_gauge_->Add(labels).Set(value);
        return;
      case GaugeId::STRING_POOL_RECLAIMED_BYTES:
        family_string_pool_reclaimed_bytes_gauge_->Add(labels).Set(value);
        return;
      case GaugeId::STRING_POOL_USED_BYTES:
        family_string_pool_used_bytes_gauge_->Add(labels).Set(value);
        return;
    }

    spdlog::warn("Unknown gauge metric id {}", id);
  }

} /* metrics */
} /* mhconfig */
