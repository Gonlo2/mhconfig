#ifndef MHCONFIG__WORKER_H
#define MHCONFIG__WORKER_H

#include <thread>
#include <random>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "mhconfig/metrics.h"
#include "mhconfig/command.h"

namespace mhconfig
{

class Worker : public ::jmutils::Worker<Worker, WorkerCommandRef>
{
public:
  Worker(
    context_t* ctx
  );

  virtual ~Worker();

private:
  friend class ::jmutils::Worker<Worker, WorkerCommandRef>;

  context_t* ctx_;

  void on_start() noexcept {
  }

  inline bool pop(
    WorkerCommandRef& command
  ) noexcept {
    ctx_->worker_queue.pop(command);
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

  inline bool execute(WorkerCommandRef&& command) {
    return command->execute(ctx_);
  }

  inline void loop_stats(
    std::string& name,
    jmutils::MonotonicTimePoint start_time,
    jmutils::MonotonicTimePoint end_time
  ) noexcept {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    ctx_->metrics.add(
      Metrics::Id::WORKER_DURATION_NANOSECONDS,
      {{"type", name}},
      duration_ns
    );
  }

  void on_stop() noexcept {
  }
};

} /* mhconfig */

#endif
