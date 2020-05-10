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

using jmutils::container::Queue;

class Worker : public ::jmutils::parallelism::Worker<Worker, command::CommandRef>
{
public:
  Worker(
    Queue<command::CommandRef>& worker_queue,
    size_t num_threads,
    command::Command::context_t context
  );

  virtual ~Worker();

private:
  friend class jmutils::parallelism::Worker<Worker, command::CommandRef>;

  command::Command::context_t context_;

  inline bool metricate(
    command::CommandRef& command,
    uint_fast32_t sequential_id
  ) {
    return command->force_take_metric() || ((sequential_id & 0xfff) == 0);
  }

  inline void loop_stats(
    command::CommandRef& command,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    context_.metrics_service.observe(
      metrics::MetricsService::MetricId::WORKER_DURATION_NANOSECONDS,
      {{"type", command->name()}},
      duration_ns
    );
  }

  inline bool process_command(
    command::CommandRef command
  ) {
    return command->execute(context_);
  }

};

} /* worker */
} /* mhconfig */

#endif
