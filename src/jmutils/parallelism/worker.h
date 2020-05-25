#ifndef JMUTILS__PARALLELISM__WORKER_H
#define JMUTILS__PARALLELISM__WORKER_H

#include <thread>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "jmutils/container/queue.h"
#include "jmutils/time.h"

namespace jmutils
{
namespace parallelism
{

template <typename Parent, typename Command>
class Worker
{
public:
  Worker()
    : thread_(nullptr)
  {
  }

  virtual ~Worker() {
  }

  Worker(const Worker& o) = delete;

  Worker(Worker&& o)
    : thread_(std::move(o.thread_))
  {
  }

  bool start() {
    thread_ = std::make_unique<std::thread>(&Worker::run, this);
    return true;
  }

  void join() {
    if (thread_ != nullptr) {
      thread_->join();
    }
  }

private:
  std::unique_ptr<std::thread> thread_;

  void run() {
    spdlog::debug("Started the worker");

    uint_fast32_t sequential_id = 0;

    while (true) {
      spdlog::debug("The worker is waiting for a command");
      Command command;
      static_cast<Parent*>(this)->pop(command);

      std::string command_name = command->name();
      if (static_cast<Parent*>(this)->metricate(command, sequential_id)) {
        auto start_time = jmutils::time::monotonic_now();
        execute_command(command_name, std::move(command));
        auto end_time = jmutils::time::monotonic_now();

        static_cast<Parent*>(this)->loop_stats(
          command_name,
          start_time,
          end_time
        );
      } else {
        execute_command(command_name, std::move(command));
      }

      sequential_id = (sequential_id+1) & 0xefffffff;
    }

    spdlog::debug("Finished the worker");
  }

  inline bool execute_command(std::string& name, Command&& command) {
    try {
      spdlog::debug("Received a {} command", name);
      bool ok = static_cast<Parent*>(this)
        ->process_command(std::move(command));
      if (!ok) {
        spdlog::error("Can't process a {} command", name);
      }
      return ok;
    } catch (const std::exception &e) {
      spdlog::error(
        "Some error take place processing the command {}: {}",
        name,
        e.what()
      );
    } catch (...) {
      spdlog::error(
        "Some unknown error take place processing the command {}",
        name
      );
    }

    return false;
  }

};

}
/* parallelism */
} /* jmutils */

#endif /* ifndef JMUTILS__PARALLELISM__WORKER_H */
