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

    family_asked_configs_gauge_ = &prometheus::BuildGauge()
      .Name("asked_configs")
      .Help("The asked configs")
      .Register(*registry_);

    family_registered_watchers_gauge_ = &prometheus::BuildGauge()
      .Name("registered_watchers")
      .Help("The registered watchers")
      .Register(*registry_);
  }

  void SyncMetricsService::add(
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
      case MetricId::OPTIMIZED_MERGED_CONFIG_USED_BYTES:
        family_optimized_merged_config_used_bytes_summary_->Add(labels, quantiles_)
          .Observe(value);
        return;
      case MetricId::STRING_POOL_NUM_STRINGS:
        family_string_pool_num_strings_gauge_->Add(labels)
          .Set(value);
        return;
      case MetricId::STRING_POOL_NUM_CHUNKS:
        family_string_pool_num_chunks_gauge_->Add(labels)
          .Set(value);
        return;
      case MetricId::STRING_POOL_RECLAIMED_BYTES:
        family_string_pool_reclaimed_bytes_gauge_->Add(labels)
          .Set(value);
        return;
      case MetricId::STRING_POOL_USED_BYTES:
        family_string_pool_used_bytes_gauge_->Add(labels)
          .Set(value);
        return;
      case MetricId::ASKED_CONFIGS:
        family_asked_configs_gauge_->Add(labels)
          .Set(value);
        return;
      case MetricId::REGISTERED_WATCHERS:
        family_registered_watchers_gauge_->Add(labels)
          .Set(value);
        return;
    }

    spdlog::warn("Unknown metric id {} to add", id);
  }

  void SyncMetricsService::clear(
    MetricId id
  ) {
    switch (id) {
      case MetricId::API_DURATION_NANOSECONDS:
        family_api_duration_summary_->Clear();
        return;
      case MetricId::SCHEDULER_DURATION_NANOSECONDS:
        family_scheduler_duration_summary_->Clear();
        return;
      case MetricId::WORKER_DURATION_NANOSECONDS:
        family_worker_duration_summary_->Clear();
        return;
      case MetricId::OPTIMIZED_MERGED_CONFIG_USED_BYTES:
        family_optimized_merged_config_used_bytes_summary_->Clear();
        return;
      case MetricId::STRING_POOL_NUM_STRINGS:
        family_string_pool_num_strings_gauge_->Clear();
        return;
      case MetricId::STRING_POOL_NUM_CHUNKS:
        family_string_pool_num_chunks_gauge_->Clear();
        return;
      case MetricId::STRING_POOL_RECLAIMED_BYTES:
        family_string_pool_reclaimed_bytes_gauge_->Clear();
        return;
      case MetricId::STRING_POOL_USED_BYTES:
        family_string_pool_used_bytes_gauge_->Clear();
        return;
      case MetricId::ASKED_CONFIGS:
        family_asked_configs_gauge_->Clear();
        return;
      case MetricId::REGISTERED_WATCHERS:
        family_registered_watchers_gauge_->Clear();
        return;
    }

    spdlog::warn("Unknown metric id {} to clear", id);
  }

  void SyncMetricsService::set_namespaces_metrics(
    std::vector<namespace_metrics_t>&& namespaces_metrics
  ) {
    clear(MetricId::ASKED_CONFIGS);
    clear(MetricId::REGISTERED_WATCHERS);

    for (const auto& namespace_metrics : namespaces_metrics) {
      for (const auto& it : namespace_metrics.asked_configs) {
        add(
          MetricId::ASKED_CONFIGS,
          {
            {"root_path", namespace_metrics.name},
            {"override", it.first.first},
            {"name", it.first.second}
          },
          it.second.v
        );
      }

      absl::flat_hash_map<std::pair<std::string, std::string>, ::jmutils::zero_value_t<uint32_t>> watchers;
      for (const auto& ww : namespace_metrics.watchers) {
        if (auto w = ww.lock()) {
          for (const auto& override_ : w->overrides()) {
            watchers[std::make_pair(override_, w->document())].v += 1;
            if (!w->template_().empty()) {
              watchers[std::make_pair(override_, w->template_())].v += 1;
            }
          }
        }
      }

      for (const auto& it : watchers) {
        add(
          MetricId::REGISTERED_WATCHERS,
          {
            {"root_path", namespace_metrics.name},
            {"override", it.first.first},
            {"name", it.first.second}
          },
          it.second.v
        );
      }
    }
  }

} /* metrics */
} /* mhconfig */
