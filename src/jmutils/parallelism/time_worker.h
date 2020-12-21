#ifndef JMUTILS__PARALLELISM__TIME_WORKER_H
#define JMUTILS__PARALLELISM__TIME_WORKER_H

#include "jmutils/container/queue.h"
#include "jmutils/parallelism/worker.h"
#include "jmutils/time.h"

#include <absl/container/flat_hash_map.h>

namespace jmutils
{

namespace {
  struct time_event_t {
    bool close{false};
    uint32_t tag;
    uint64_t execute_at_ms;
    std::unique_ptr<std::function<uint64_t()>> function{nullptr};

    time_event_t(
      uint32_t tag_,
      uint64_t execute_at_ms_,
      std::unique_ptr<std::function<uint64_t()>>&& function_
    ) : tag(tag_),
      execute_at_ms(execute_at_ms_),
      function(std::move(function_))
    {
    }
  };
}

class TimeWorker : public Worker<TimeWorker, time_event_t*>
{
public:
  TimeWorker() {
  }

  virtual ~TimeWorker() {
  }

  void set_function(uint32_t tag, uint64_t execute_at_ms, std::function<uint64_t()>&& function) {
    queue_.push(
      std::make_unique<time_event_t>(
        tag,
        execute_at_ms,
        std::make_unique<std::function<uint64_t()>>(std::move(function))
      )
    );
  }

private:
  typedef time_event_t* event_type;

  friend class Worker<TimeWorker, event_type>;

  container::Queue<std::unique_ptr<time_event_t>> queue_;
  absl::flat_hash_map<uint32_t, std::unique_ptr<time_event_t>> event_by_tag_;

  inline void on_start() {
  }

  inline bool pop(event_type& event) {
    while (true) {
      std::unique_ptr<time_event_t> event_tmp;
      if (event_by_tag_.empty()) {
        queue_.pop(event_tmp);
      } else {
        event = event_by_tag_.begin()->second.get();
        for (auto& e : event_by_tag_) {
          if (e.second->execute_at_ms < event->execute_at_ms) {
            event = e.second.get();
          }
        }
        auto now_ms = monotonic_now_ms();
        if (event->execute_at_ms <= now_ms) break;
        if (!queue_.pop_or_wait_for(event_tmp, event->execute_at_ms - now_ms)) break;
      }

      if (event_tmp->close) return false;
      if (event_tmp->function == nullptr) {
        event_by_tag_.erase(event_tmp->tag);
      } else {
        event_by_tag_[event_tmp->tag] = std::move(event_tmp);
      }
    }

    return true;
  }

  inline bool metricate(event_type, uint_fast32_t) const {
    return false;
  }

  inline std::string event_name(event_type) const {
    return "";
  }

  inline bool execute(event_type event) {
    try {
      uint64_t execute_at_ms = (*event->function)();
      if (execute_at_ms) {
        event->execute_at_ms = execute_at_ms;
      } else {
        event_by_tag_.erase(event->tag);
      }
      return true;
    } catch (const std::exception &e) {
      spdlog::error(
        "Some error take place executing a time event with tag {}, removing it!: {}",
        event->tag,
        e.what()
      );
    } catch (...) {
      spdlog::error(
        "Some unknown error take place executing a time event with tag {}, removing it!",
        event->tag
      );
    }

    event_by_tag_.erase(event->tag);
    return false;
  }

  inline void loop_stats(
    std::string& name,
    jmutils::MonotonicTimePoint start_time,
    jmutils::MonotonicTimePoint end_time
  ) {
  }

  inline void on_stop() {
  }

};

} /* jmutils */

#endif /* ifndef JMUTILS__PARALLELISM__TIME_WORKER_H */
