#include "mhconfig/worker.h"

namespace mhconfig
{

bool is_worker_thread(bool value) {
  thread_local static bool is_wt{value};
  return is_wt;
}

bool execute_command_in_worker_thread(
  WorkerCommandRef&& command,
  context_t* ctx
) {
  if (is_worker_thread()) {
    spdlog::trace("Executing command '{}' in the local thread", command->name());
    return execute_command(std::move(command), ctx);
  }

  spdlog::trace("Executing command '{}' in a worker thread", command->name());
  ctx->worker_queue.push(std::move(command));
  return true;
}

bool execute_command(
  WorkerCommandRef&& command,
  context_t* ctx
) {
  bool ok;
  if (command->force_take_metric()) {
    std::string name = command->name();

    auto start_time = jmutils::monotonic_now();
    ok = command->execute(ctx);
    auto end_time = jmutils::monotonic_now();

    if (!ok) {
      spdlog::error("Some error take place processing the event '{}'", name);
    }

    record_command_stats(name, start_time, end_time, ctx);
  } else {
    ok = command->execute(ctx);
    if (!ok) {
      spdlog::error("Some error take place processing a event");
    }
  }
  return ok;
}

Worker::~Worker() {
}

} /* mhconfig */
