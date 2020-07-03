#ifndef MHCONFIG__WORKER__MOD_H
#define MHCONFIG__WORKER__MOD_H

#include <thread>
#include <random>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/worker/command/command.h"

namespace mhconfig
{
namespace worker
{

class Worker : public ::jmutils::Worker<Worker, command::CommandRef>
{
public:
  Worker(
    WorkerQueue::ReceiverRef&& worker_queue,
    command::Command::context_t&& context
  );

  virtual ~Worker();

private:
  friend class ::jmutils::Worker<Worker, command::CommandRef>;

  WorkerQueue::ReceiverRef worker_queue_;
  command::Command::context_t context_;

  void on_start() noexcept {
  }

  inline bool pop(
    command::CommandRef& command
  ) noexcept {
    worker_queue_->pop(command);
    return true;
  }

  inline bool metricate(
    command::CommandRef& command,
    uint_fast32_t sequential_id
  ) noexcept {
    return command->force_take_metric() || ((sequential_id & 0xfff) == 0);
  }

  inline std::string event_name(
    command::CommandRef& command
  ) noexcept {
    return command->name();
  }

  inline bool execute(command::CommandRef&& command) noexcept {
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

} /* worker */
} /* mhconfig */

#endif
