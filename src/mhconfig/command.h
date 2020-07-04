#ifndef MHCONFIG__COMMAND_H
#define MHCONFIG__COMMAND_H

#include <memory>

#include "jmutils/container/mpsc_queue.h"
#include "jmutils/container/spmc_queue.h"

#include "mhconfig/config_namespace.h"
#include "mhconfig/metrics/metrics_service.h"

namespace mhconfig
{

namespace
{
  static const std::string EMPTY_STRING{""};
}


class WorkerCommand;
typedef std::unique_ptr<WorkerCommand> WorkerCommandRef;

class SchedulerCommand;
typedef std::unique_ptr<SchedulerCommand> SchedulerCommandRef;

typedef jmutils::container::MPSCQueue<SchedulerCommandRef, 12> SchedulerQueue;
typedef jmutils::container::SPMCQueue<WorkerCommandRef, 12> WorkerQueue;

class SchedulerCommand
{
public:
  enum class CommandType {
    ADD_NAMESPACE,
    GET_NAMESPACE_BY_PATH,
    GET_NAMESPACE_BY_ID,
    GENERIC
  };

  enum class CommandResult {
    OK,
    ERROR,
    SOFTDELETE_NAMESPACE
  };

  struct context_t {
    WorkerQueue& worker_queue;
    std::unique_ptr<metrics::MetricsService> metrics;

    absl::flat_hash_map<std::string, std::shared_ptr<config_namespace_t>> namespace_by_path;
    absl::flat_hash_map<uint64_t, std::shared_ptr<config_namespace_t>> namespace_by_id;
    absl::flat_hash_map<std::string, std::vector<SchedulerCommandRef>> commands_waiting_for_namespace_by_path;

    context_t(
      WorkerQueue& worker_queue_,
      std::unique_ptr<metrics::MetricsService>&& metrics_
    )
      : worker_queue(worker_queue_),
      metrics(std::move(metrics_))
    {}
  };

  virtual ~SchedulerCommand();

  virtual std::string name() const = 0;

  virtual CommandType type() const = 0;

  virtual const std::string& namespace_path() const;
  virtual uint64_t namespace_id() const;
  virtual const std::shared_ptr<config_namespace_t> config_namespace() const;

  virtual CommandResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  );

  virtual bool on_get_namespace_error(
    WorkerQueue& worker_queue
  );

  virtual bool execute(
    context_t& context
  );
};

class WorkerCommand
{
public:
  struct context_t {
    SchedulerQueue::SenderRef scheduler_queue;
    std::unique_ptr<metrics::MetricsService> async_metrics_service;
    metrics::MetricsService& metrics_service;

    context_t(
      SchedulerQueue::SenderRef&& scheduler_queue_,
      std::unique_ptr<metrics::MetricsService> async_metrics_service_,
      metrics::MetricsService& metrics_service_
    )
      : scheduler_queue(std::move(scheduler_queue_)),
      async_metrics_service(std::move(async_metrics_service_)),
      metrics_service(metrics_service_)
    {}
  };

  virtual ~WorkerCommand();

  virtual std::string name() const = 0;

  virtual bool force_take_metric() const;

  virtual bool execute(context_t& context) = 0;
};

} /* mhconfig */

#endif
