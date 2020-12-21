#ifndef JMUTILS__PARALLELISM__WORKER_H
#define JMUTILS__PARALLELISM__WORKER_H

#include <assert.h>
#include <bits/exception.h>
#include <bits/types/struct_sched_param.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <thread>

#include "jmutils/time.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace jmutils
{

template <typename Parent, typename Event>
class Worker
{
public:
  Worker() {
  }

  virtual ~Worker() {
    assert(thread_ == nullptr);
  }

  Worker(const Worker& o) = delete;

  Worker(Worker&& o) noexcept
    : thread_(std::move(o.thread_))
  {
    o.thread_ = nullptr;
  }

  bool start() {
    if (thread_ != nullptr) return false;
    thread_ = std::make_unique<std::thread>(&Worker::run, this);
    return true;
  }

  bool join() {
    if (thread_ == nullptr) return false;
    thread_->join();
    thread_ = nullptr;
    return true;
  }

private:
  std::unique_ptr<std::thread> thread_{nullptr};

  void run() {
    spdlog::trace("Starting the worker {}", (void*)this);
    static_cast<Parent*>(this)->on_start();

    std::string name;
    uint_fast32_t sequential_id = 0;

    while (true) {
      Event event;
      spdlog::trace("The worker is waiting for a command");
      if (!static_cast<Parent*>(this)->pop(event)) {
        spdlog::trace("The queue has been closed");
        break;
      }

      if (static_cast<Parent*>(this)->metricate(event, sequential_id++)) {
        name = static_cast<Parent*>(this)->event_name(event);

        auto start_time = jmutils::monotonic_now();
        bool ok = safe_execute(std::move(event));
        auto end_time = jmutils::monotonic_now();

        if (!ok) {
          spdlog::error("Some error take place processing the event '{}'", name);
        }

        static_cast<Parent*>(this)->loop_stats(name, start_time, end_time);
      } else {
        if (!safe_execute(std::move(event))) {
          spdlog::error("Some error take place processing a event");
        }
      }
    }

    spdlog::trace("Stoping the worker {}", (void*)this);
    static_cast<Parent*>(this)->on_stop();
  }

  inline bool safe_execute(Event&& event) noexcept {
    try {
      return static_cast<Parent*>(this)->execute(std::move(event));
    } catch (const std::exception &e) {
      spdlog::error("Some error take place executing a event: {}", e.what());
    } catch (...) {
      spdlog::error("Some unknown error take place executing a event");
    }

    return false;
  }
};

} /* jmutils */

#endif /* ifndef JMUTILS__PARALLELISM__WORKER_H */
