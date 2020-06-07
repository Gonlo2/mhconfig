#ifndef MHCONFIG__METRICS__METRICS_WORKER_H
#define MHCONFIG__METRICS__METRICS_WORKER_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "string_pool/pool.h"
#include "mhconfig/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/setup_command.h"

namespace mhconfig
{
namespace metrics
{

class AsyncMetric
{
public:
  AsyncMetric() {
  }
  virtual ~AsyncMetric() {
  }

  virtual std::string name() = 0;

  virtual void apply(
    metrics::MetricsService& metrics
  ) = 0;
};

class AddMetric : public AsyncMetric
{
public:
  AddMetric(
    metrics::MetricsService::MetricId id,
    std::map<std::string, std::string>&& labels,
    double value
  )
    : id_(id),
    labels_(labels),
    value_(value)
  {
  }

  virtual ~AddMetric() {
  }

  std::string name() override {
    return "ADD_METRIC";
  }

  void apply(
    metrics::MetricsService& metrics
  ) override {
    metrics.add(
      id_,
      std::move(labels_),
      value_
    );
  }

private:
  metrics::MetricsService::MetricId id_;
  std::map<std::string, std::string> labels_;
  double value_;
};

class SetNamespacesMetrics : public AsyncMetric
{
public:
  SetNamespacesMetrics(
    std::vector<MetricsService::namespace_metrics_t>&& namespaces_metrics
  )
    : namespaces_metrics_(std::move(namespaces_metrics))
  {
  }

  virtual ~SetNamespacesMetrics() {
  }

  std::string name() override {
    return "SET_NAMESPACES_METRICS";
  }

  void apply(
    metrics::MetricsService& metrics
  ) override {
    metrics.set_namespaces_metrics(std::move(namespaces_metrics_));
  }

private:
  std::vector<MetricsService::namespace_metrics_t> namespaces_metrics_;
};


typedef jmutils::container::MPSCQueue<std::unique_ptr<AsyncMetric>, 12> MetricsQueue;

//TODO
class MetricsWorker : public jmutils::parallelism::Worker<MetricsWorker, std::unique_ptr<AsyncMetric>>
{
public:
  MetricsWorker(
    MetricsQueue& queue,
    metrics::MetricsService& metrics
  )
    : queue_(queue),
    metrics_(metrics)
  {
  }

  virtual ~MetricsWorker() {
  }

private:
  friend class jmutils::parallelism::Worker<MetricsWorker, std::unique_ptr<AsyncMetric>>;

  MetricsQueue& queue_;
  metrics::MetricsService& metrics_;

  inline void pop(
    std::unique_ptr<AsyncMetric>& command
  ) {
    queue_.pop(command);
  }

  inline bool metricate(
    std::unique_ptr<AsyncMetric>& command,
    uint_fast32_t sequential_id
  ) {
    return (sequential_id & 0xfff) == 0;
  }

  inline void loop_stats(
    std::string& command_name,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    metrics_.add(
      metrics::MetricsService::MetricId::WORKER_DURATION_NANOSECONDS,
      {{"type", command_name}},
      duration_ns
    );
  }

  inline bool process_command(
    std::unique_ptr<AsyncMetric>&& command
  ) {
    command->apply(metrics_);
    return true;
  }

};

} /* metrics */
} /* mhconfig */

#endif
