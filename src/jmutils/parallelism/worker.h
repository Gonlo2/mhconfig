#ifndef JMUTILS__PARALLELISM__WORKER_H
#define JMUTILS__PARALLELISM__WORKER_H

#include <thread>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "jmutils/container/queue.h"

namespace jmutils
{
namespace parallelism
{

template <typename Command>
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
    : logger_(o.logger_),
    input_queue_(o.input_queue_),
    num_threads_(o.num_threads_),
    threads_(std::move(o.threads_))
  {
  }

  bool start() {
    if (!pre_start()) return false;

    threads_.reserve(num_threads_);
    for (size_t i = num_threads_; i; --i) {
      logger_->debug(
        "Starting the thread {} of {} worker with the queue {}",
        i,
        worker_name(),
        (uint64_t) &input_queue_
      );

      threads_.push_back(
        std::make_unique<std::thread>(&Worker::run, this)
      );
    }

    return true;
  }

  void join() {
    for (auto& thread: threads_) thread->join();
  }

protected:
  enum ProcessResult {
    OK,
    MISSING,
    ERROR
  };

  std::shared_ptr<spdlog::logger> logger_{spdlog::get("console")};

  virtual bool pre_start() {
    return true;
  }

private:
  jmutils::container::Queue<Command>& input_queue_;
  size_t num_threads_;

  std::vector<std::unique_ptr<std::thread>> threads_;

  virtual const std::string worker_name() const = 0;

  void run() {
    logger_->debug(
      "Started a {} worker with the queue {}",
      worker_name(),
      (uint64_t) &input_queue_
    );

    while (true) {
      logger_->debug(
        "A {} worker is waiting for a command",
        worker_name()
      );
      auto command = input_queue_.pop();

      try {
        logger_->debug(
          "A {} worker received a {} command",
          worker_name(),
          to_string(command.type)
        );

        ProcessResult process_result = worker_process(command);
        switch (process_result) {
          case ProcessResult::OK:
            logger_->debug(
              "A {} worker process correctly a {} command",
              worker_name(),
              to_string(command.type)
            );
            break;

          case ProcessResult::MISSING:
            logger_->warn(
              "A {} worker can't process a {} command",
              worker_name(),
              to_string(command.type)
            );
            break;

          case ProcessResult::ERROR:
            logger_->error(
              "A {} worker fail to process a {} command",
              worker_name(),
              to_string(command.type)
            );
            break;

          default:
            logger_->warn(
              "A {} worker return a unknown command result for a {} command",
              worker_name(),
              to_string(command.type)
            );
        }
      } catch (const std::exception &e) {
        logger_->error(
          "Some error take place processing the {} worker command {}: {}",
          worker_name(),
          to_string(command.type),
          e.what()
        );
      } catch (...) {
        logger_->error(
          "Some unknown error take place processing the {} worker command {}",
          worker_name(),
          to_string(command.type)
        );
      }
    }

    logger_->debug(
      "Finished a {} worker with the queue {}",
      worker_name(),
      (uint64_t) &input_queue_
    );
  }

  virtual ProcessResult worker_process(const Command& command) = 0;

};

}
/* parallelism */
} /* jmutils */


#endif /* ifndef JMUTILS__PARALLELISM__WORKER_H */
