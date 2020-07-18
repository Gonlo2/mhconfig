#ifndef JMUTILS__PARALLELISM__WORKER_H
#define JMUTILS__PARALLELISM__WORKER_H

#include <thread>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "jmutils/time.h"
#include <errno.h>

namespace jmutils
{

template <typename Parent, typename Event>
class Worker
{
public:
  enum class Scope {
    SYSTEM,
    PROCESS
  };

  enum class SchedulingPolicy {
    FIFO,
    RR,
    OTHER
  };

  Worker() {
  }

  virtual ~Worker() {
    assert(!running_);
  }

  Worker(const Worker& o) = delete;

  Worker(Worker&& o) noexcept
    : thread_(std::move(o.thread_)),
    attr_(std::move(o.attr_)),
    running_(o.running_),
    use_attr_(o.use_attr_)
  {
    o.running_ = false;
    o.use_attr_ = false;
  }

  template <typename T>
  bool set_affinity(T&& cpus) {
    if (!ensure_attr_is_init()) return false;

    cpu_set_t set;
    size_t num_cpus = 0;

    CPU_ZERO(&set);
    for (auto cpu : cpus) {
      CPU_SET(cpu, &set);
      ++num_cpus;
    }

    if (int res = pthread_attr_setaffinity_np(&attr_, num_cpus, &set)) {
      // TODO Improve the error messages
      spdlog::error("The pthread_attr_setaffinity_np return a error {}", res);
      return false;
    }

    return true;
  }

  bool set_scheduling_policy(SchedulingPolicy policy, int priority) {
    if (!ensure_attr_is_init()) return false;

    int res = 1;

    switch (policy) {
      case SchedulingPolicy::FIFO:
        res = pthread_attr_setschedpolicy(&attr_, SCHED_FIFO);
        break;
      case SchedulingPolicy::RR:
        res = pthread_attr_setschedpolicy(&attr_, SCHED_RR);
        break;
      case SchedulingPolicy::OTHER:
        res = pthread_attr_setschedpolicy(&attr_, SCHED_OTHER);
        break;
    }
    if (res) {
      spdlog::error("The pthread_attr_setschedpolicy return a error {}", res);
      return false;
    }

    struct sched_param param;
    param.sched_priority = priority;

    res = pthread_attr_setschedparam(&attr_, &param);
    if (res) {
      spdlog::error("The pthread_attr_setschedparam return a error {}", res);
      return false;
    }

    return true;
  }

  bool set_scope(Scope scope) {
    if (!ensure_attr_is_init()) return false;

    int res = 1;
    switch (scope) {
      case Scope::SYSTEM:
        res = pthread_attr_setscope(&attr_, PTHREAD_SCOPE_SYSTEM);
        break;
      case Scope::PROCESS:
        res = pthread_attr_setscope(&attr_, PTHREAD_SCOPE_PROCESS);
        break;
    }

    if (res) {
      spdlog::error("The pthread_attr_setscope return a error {}", res);
      return false;
    }

    return true;
  }

  bool start() {
    pthread_attr_t* attr = use_attr_ ? &attr_ : nullptr;

    if (int res = pthread_create(&thread_, attr, (void* (*)(void*)) &Worker::run, this)) {
      spdlog::error("The pthread_create return a error {}", res);
      return false;
    }

    running_ = true;

    if (use_attr_) {
      use_attr_ = false;
      if (int res = pthread_attr_destroy(&attr_)) {
        spdlog::error("The pthread_attr_destroy return a error {}", res);
        return false;
      }
    }

    return true;
  }

  bool join() {
    if (!running_) return false;
    running_ = false;

    // Maybe it's a good idea check if the thread exits gracefully
    if (int res = pthread_join(thread_, nullptr)) {
      spdlog::error("The pthread_join return a error {}", res);
      return false;
    }

    return true;
  }

private:
  pthread_t thread_;
  pthread_attr_t attr_;
  bool running_{false};
  bool use_attr_{false};

  bool ensure_attr_is_init() {
    if (!use_attr_) {
      if (int res = pthread_attr_init(&attr_)) {
        spdlog::error("The pthread_attr_init return a error {}", res);
        return false;
      }

      if (int res = pthread_attr_setinheritsched(&attr_, PTHREAD_EXPLICIT_SCHED)) {
        spdlog::error("The pthread_attr_setinheritsched return a error {}", res);
        return false;
      }
    }

    return true;
  }

  void* run(void* _) noexcept {
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

      if (static_cast<Parent*>(this)->metricate(event, sequential_id)) {
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

      sequential_id = (sequential_id+1) & 0xefffffff;
    }

    spdlog::trace("Stoping the worker {}", (void*)this);
    static_cast<Parent*>(this)->on_stop();

    return nullptr;
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
