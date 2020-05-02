#include "mhconfig/metrics.h"

namespace mhconfig
{
  Metrics::Metrics(const std::string& address) :
    exposer_(address),
    metric_id_(0)
  {}

  Metrics::~Metrics() {
  }

  void Metrics::init() {
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

    family_serialization_duration_summary_ = &prometheus::BuildSummary()
      .Name("serialization_duration_nanoseconds")
      .Help("How many nanoseconds takes serializate the asked config")
      .Register(*registry_);
  }

  void Metrics::api_duration(const std::string& type, double duration_ns) {
    //family_api_duration_summary_->Add({{"type", type}}, quantiles_)
      //.Observe(duration_ns);
  }

  void Metrics::scheduler_duration(const std::string& type, double duration_ns) {
    //family_scheduler_duration_summary_->Add({{"type", type}}, quantiles_)
      //.Observe(duration_ns);
  }

  void Metrics::worker_duration(const std::string& type, double duration_ns) {
    //family_worker_duration_summary_->Add({{"type", type}}, quantiles_)
      //.Observe(duration_ns);
  }

  void Metrics::serialization_duration(double duration_ns) {
    //family_serialization_duration_summary_->Add({}, quantiles_)
      //.Observe(duration_ns);
  }
} /* mhconfig */
