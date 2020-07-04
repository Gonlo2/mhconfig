#ifndef MHCONFIG__WORKER_H
#define MHCONFIG__WORKER_H

#include <thread>
#include <random>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/command.h"

namespace mhconfig
{

class Worker : public ::jmutils::Worker<Worker, WorkerCommandRef>
{
public:
  Worker(
    WorkerQueue::ReceiverRef&& worker_queue,
    WorkerCommand::context_t&& context
  );

  virtual ~Worker();

private:
  friend class ::jmutils::Worker<Worker, WorkerCommandRef>;

  WorkerQueue::ReceiverRef worker_queue_;
  WorkerCommand::context_t context_;

  void on_start() noexcept {
  }

  inline bool pop(
    WorkerCommandRef& command
  ) noexcept {
    worker_queue_->pop(command);
    return true;
  }

  inline bool metricate(
    WorkerCommandRef& command,
    uint_fast32_t sequential_id
  ) noexcept {
    return command->force_take_metric() || ((sequential_id & 0xfff) == 0);
  }

  inline std::string event_name(
    WorkerCommandRef& command
  ) noexcept {
    return command->name();
  }

  inline bool execute(WorkerCommandRef&& command) noexcept {
    try {
      return command->execute(context_);
    } catch (const std::exception &e) {
      spdlog::error("Some error take place processing a command: {}", e.what());
    } catch (...) {
      spdlog::error("Some unknown error take place processing a command");
    }

    return false;
  }

  inline void loop_stats(
    std::string& name,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) noexcept {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    context_.async_metrics_service->add(
      metrics::MetricsService::MetricId::WORKER_DURATION_NANOSECONDS,
      {{"type", name}},
      duration_ns
    );
  }

  void on_stop() noexcept {
  }
};

} /* mhconfig */

#endif
