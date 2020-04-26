#ifndef JMUTILS__STRUCTURES__THREAD_SAFE_QUEUE_H
#define JMUTILS__STRUCTURES__THREAD_SAFE_QUEUE_H

#include <stdlib.h>
#include <memory>
#include <queue>
#include <thread>
#include <mutex>
#include <utility>
#include <condition_variable>

#include <iostream>
#include <exception>

namespace jmutils
{
namespace container
{

template <typename T>
class Queue
{
public:
  Queue() {
  }
  virtual ~Queue() {
  }

  Queue(const Queue&) = delete;
  Queue(Queue&&) = delete;

  T pop() {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) cond_.wait(mlock);

    T item = queue_.front();
    queue_.pop();
    return item;
  }

  std::pair<bool, T> pop_before(
    const std::chrono::high_resolution_clock::time_point& t
  ) {
    std::pair<bool, T> result;

    std::unique_lock<std::mutex> mlock(mutex_);

    while (queue_.empty()) {
      if (cond_.wait_until(mlock, t) == std::cv_status::timeout) break;
    }

    result.first = !queue_.empty();
    if (result.first) {
      result.second = queue_.front();
      queue_.pop();
    }

    return result;
  }

  void push(const T item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(item);
    mlock.unlock();
    cond_.notify_one();
  }

  template <typename Container>
  void push(const Container items) {
    std::unique_lock<std::mutex> mlock(mutex_);
    for (const auto& x : items) queue_.push(x);
    mlock.unlock();
    for (const auto& x : items) cond_.notify_one();
  }

protected:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

} /* container */
} /* jmutils */

#endif
