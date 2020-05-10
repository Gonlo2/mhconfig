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
  Worker(
    jmutils::container::Queue<Command>& input_queue,
    size_t num_threads
  ) : input_queue_(input_queue),
    num_threads_(num_threads)
  {
  }

  virtual ~Worker() {
  }

  Worker(const Worker& o) = delete;

  Worker(Worker&& o)
    : input_queue_(o.input_queue_),
    num_threads_(o.num_threads_),
    threads_(std::move(o.threads_))
  {
  }

  bool start() {
    if (!pre_start()) return false;

    threads_.reserve(num_threads_);
    for (size_t i = num_threads_; i; --i) {
      spdlog::debug("Starting the thread {}", i);
      threads_.push_back(std::make_unique<std::thread>(&Worker::run, this));
    }

    return true;
  }

  void join() {
    for (auto& thread: threads_) thread->join();
  }

protected:
  jmutils::container::Queue<Command>& input_queue_;

  virtual bool pre_start() {
    return true;
  }

private:
  size_t num_threads_;

  std::vector<std::unique_ptr<std::thread>> threads_;

  void run() {
    spdlog::debug("Started the worker");

    uint_fast32_t sequential_id{0};

    Command command;
    while (true) {
      spdlog::debug("The worker is waiting for a command");
      input_queue_.pop(command);

      if (static_cast<Parent*>(this)->metricate(command, sequential_id)) {
        auto start_time = jmutils::time::monotonic_now();
        execute_command(command);
        auto end_time = jmutils::time::monotonic_now();

        static_cast<Parent*>(this)->loop_stats(command, start_time, end_time);
      } else {
        execute_command(command);
      }

      sequential_id = (sequential_id+1) & 0xefffffff;
    }

    spdlog::debug("Finished the worker");
  }

  inline bool execute_command(Command& command) {
    try {
      spdlog::debug("Received a {} command", command->name());
      bool ok = static_cast<Parent*>(this)->process_command(command);
      if (!ok) {
        spdlog::error("Can't process a {} command", command->name());
      }
      return ok;
    } catch (const std::exception &e) {
      spdlog::error(
        "Some error take place processing the command {}: {}",
        command->name(),
        e.what()
      );
    } catch (...) {
      spdlog::error(
        "Some unknown error take place processing the command {}",
        command->name()
      );
    }

    return false;
  }

};

}
/* parallelism */
} /* jmutils */

#endif /* ifndef JMUTILS__PARALLELISM__WORKER_H */
