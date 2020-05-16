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

  void pop(T& value) {
    std::unique_lock<std::mutex> mlock(mutex_);
    cond_.wait(mlock, [this]{ return !queue_.empty(); });

    std::swap(queue_.front(), value);
    queue_.pop();
  }

  void push(T&& item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(std::move(item));
    mlock.unlock();
    cond_.notify_one();
  }

protected:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

} /* container */
} /* jmutils */

#endif
