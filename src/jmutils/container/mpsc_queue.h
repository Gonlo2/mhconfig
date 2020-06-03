#ifndef JMUTILS__CONTAINER__MPSC_QUEUE_H
#define JMUTILS__CONTAINER__MPSC_QUEUE_H

#include <stdlib.h>
#include <memory>
#include <queue>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <atomic>
#include <array>
#include <vector>
#include <condition_variable>

#include <iostream>
#include <exception>

namespace jmutils
{
namespace container
{

// A Multiple Producer Single Consumer queue
template <typename T, uint64_t SizeLog2>
class MPSCQueue final
{
public:
  class Sender final
  {
  public:
    Sender(
      std::shared_mutex& empty_mutex,
      std::condition_variable_any& empty_cond
    )
      : producer_size_(0),
      consumer_size_(0),
      start_(0),
      end_(0),
      empty_mutex_(empty_mutex),
      empty_cond_(empty_cond)
    {
      size_.store(0, std::memory_order_seq_cst);
    }

    ~Sender() {
    }

    bool push(T&& value) {
      if (!optimistic_push(value)) {
        std::unique_lock lock(full_mutex_);
        full_cond_.wait(lock, [this, &value]{ return optimistic_push(value); });
      }
      return true;
    }

  private:
    friend class MPSCQueue<T, SizeLog2>;

    size_t producer_size_;
    size_t consumer_size_;
    size_t start_;
    size_t end_;
    std::atomic<size_t> size_;

    std::shared_mutex& empty_mutex_;
    std::condition_variable_any& empty_cond_;

    std::mutex full_mutex_;
    std::condition_variable full_cond_;

    std::array<T, (1ul<<SizeLog2)> data_;

    inline bool optimistic_push(T& value) {
      if (producer_size_ == (1ul<<SizeLog2)) {
        producer_size_ = size_.load(std::memory_order_acquire);
      }

      if (producer_size_ != (1ul<<SizeLog2)) {
        std::shared_lock lock(empty_mutex_);
        std::swap(data_[end_], value);
        size_.fetch_add(1, std::memory_order_relaxed);
        lock.unlock();
        empty_cond_.notify_one();
        end_ = (end_+1) & ((1<<SizeLog2)-1);
        producer_size_ += 1;
        return true;
      }
      return false;
    }
  };

  typedef std::shared_ptr<Sender> SenderRef;

  MPSCQueue()
    : current_sender_(0)
  {
  }

  ~MPSCQueue() {
  }

  MPSCQueue(const MPSCQueue&) = delete;
  MPSCQueue(MPSCQueue&&) = delete;

  SenderRef new_sender() {
    auto sender = std::make_shared<Sender>(
      empty_mutex_,
      empty_cond_
    );
    senders_.push_back(sender);
    return sender;
  }

  void pop(T& value) {
    // First we try to obtain a value using the cached size
    if (!optimistic_pop(value)) {
      // If we are here that means that all the cached sizes are zero so maybe
      // the queue is empty but we need to check if someone push some value
      // after the check
      std::unique_lock lock(empty_mutex_);
      empty_cond_.wait(lock, [this, &value]{ return optimistic_pop(value); });
    }
  }

  bool push(T&& value) {
    queue_.push(std::move(value));
    return true;
  }

private:
  uint32_t current_sender_;
  std::vector<SenderRef> senders_;
  std::queue<T> queue_;
  std::shared_mutex empty_mutex_;
  std::condition_variable_any empty_cond_;

  inline bool optimistic_pop(T& value) {
    for (size_t i = senders_.size()+1; i; --i) {
      current_sender_ = (current_sender_+1) % (senders_.size()+1);
      if (current_sender_ == senders_.size()) {
        if (!queue_.empty()) {
          std::swap(queue_.front(), value);
          queue_.pop();
          return true;
        }
      } else {
        auto sender = senders_[current_sender_].get();
        if (sender->consumer_size_ == 0) {
          sender->consumer_size_ = sender->size_
            .load(std::memory_order_acquire);
        }

        if (sender->consumer_size_ != 0) {
          std::unique_lock lock(sender->full_mutex_);
          std::swap(sender->data_[sender->start_], value);
          sender->size_.fetch_sub(1, std::memory_order_relaxed);
          lock.unlock();
          sender->full_cond_.notify_one();
          sender->start_ = (sender->start_+1) & ((1<<SizeLog2)-1);
          sender->consumer_size_ -= 1;
          return true;
        }
      }
    }

    return false;
  }

};

} /* container */
} /* jmutils */

#endif
