#ifndef MHCONFIG__WORKER__MOD_H
#define MHCONFIG__WORKER__MOD_H

#include <thread>
#include <random>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "mhconfig/metrics.h"
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
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  );

  virtual ~Worker();

private:
  friend class jmutils::parallelism::Worker<Worker, command::CommandRef>;

  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;
  Metrics& metrics_;

  inline void loop_stats(
    command::CommandRef command,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    //TODO Add workr stats
    //double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      //end_time - start_time
    //).count();

    //metrics_.scheduler_duration(command->name(), duration_ns);
  }

  inline bool process_command(
    command::CommandRef command
  ) {
    return command->execute(
      scheduler_queue_,
      metrics_
    );
  }

};

} /* worker */
} /* mhconfig */

#endif
