#ifndef MHCONFIG__WORKER_H
#define MHCONFIG__WORKER_H

#include <stdint.h>
#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "jmutils/container/queue.h"
#include "jmutils/parallelism/worker.h"
#include "jmutils/time.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{

bool is_worker_thread(bool value = false);

bool execute_command_in_worker_thread(
  WorkerCommandRef&& command,
  context_t* ctx
);

bool execute_command(
  WorkerCommandRef&& command,
  context_t* ctx
);

inline void record_command_stats(
  std::string& name,
  jmutils::MonotonicTimePoint start_time,
  jmutils::MonotonicTimePoint end_time,
  context_t* ctx
) noexcept {
  double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
    end_time - start_time
  ).count();

  ctx->metrics.add(
    Metrics::Id::WORKER_DURATION_NANOSECONDS,
    {{"type", name}},
    duration_ns
  );
}

class Worker final
  : public ::jmutils::Worker<Worker, WorkerCommandRef>
{
public:
  template <typename T>
  Worker(
    T&& ctx
  ) : ctx_(std::forward<T>(ctx)) {
  };

  ~Worker();

private:
  friend class ::jmutils::Worker<Worker, WorkerCommandRef>;

  std::shared_ptr<context_t> ctx_;

  void on_start() noexcept {
    is_worker_thread(true);
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
    return command->execute(ctx_.get());
  }

  inline void loop_stats(
    std::string& name,
    jmutils::MonotonicTimePoint start_time,
    jmutils::MonotonicTimePoint end_time
  ) noexcept {
    //record_command_stats(name, start_time, end_time, ctx_.get());
  }

  void on_stop() noexcept {
  }
};

} /* mhconfig */

#endif
