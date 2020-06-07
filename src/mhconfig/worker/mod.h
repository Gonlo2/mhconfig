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

class Worker : public ::jmutils::parallelism::Worker<Worker, command::CommandRef>
{
public:
  Worker(
    WorkerQueue::ReceiverRef&& worker_queue,
    command::Command::context_t&& context
  );

  virtual ~Worker();

private:
  friend class ::jmutils::parallelism::Worker<Worker, command::CommandRef>;

  WorkerQueue::ReceiverRef worker_queue_;
  command::Command::context_t context_;

  inline void pop(
    command::CommandRef& command
  ) {
    worker_queue_->pop(command);
  }

  inline bool metricate(
    command::CommandRef& command,
    uint_fast32_t sequential_id
  ) {
    return command->force_take_metric() || ((sequential_id & 0xfff) == 0);
  }

  inline void loop_stats(
    std::string& command_name,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    context_.async_metrics_service->add(
      metrics::MetricsService::MetricId::WORKER_DURATION_NANOSECONDS,
      {{"type", command_name}},
      duration_ns
    );
  }

  inline bool process_command(
    command::CommandRef&& command
  ) {
    return command->execute(context_);
  }

};

} /* worker */
} /* mhconfig */

#endif
