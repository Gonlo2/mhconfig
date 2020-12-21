#ifndef JMUTILS__STRUCTURES__QUEUE_H
#define JMUTILS__STRUCTURES__QUEUE_H

#include <bits/stdint-uintn.h>
#include <stdlib.h>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace jmutils
{
namespace container
{

template <typename T>
class Queue
{
public:
  Queue() {}
  Queue(const Queue&) = delete;
  Queue(Queue&&) = delete;

  void pop(T& value) {
    std::unique_lock<std::mutex> mlock(mutex_);
    cond_.wait(mlock, [this]{ return !queue_.empty(); });

    value = std::move(queue_.front());
    queue_.pop_front();
  }

  bool pop_or_wait_for(T& value, uint64_t ms) {
    std::unique_lock<std::mutex> mlock(mutex_);
    bool has_value = cond_.wait_for(
      mlock,
      std::chrono::milliseconds(ms),
      [this]{ return !queue_.empty(); }
    );
    if (!has_value) return false;

    value = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }

  void push(T&& item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push_back(std::move(item));
    cond_.notify_one();
  }

protected:
  std::deque<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

} /* container */
} /* jmutils */

#endif
