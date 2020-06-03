#ifndef JMUTILS__CONTAINER__SPMC_QUEUE_H
#define JMUTILS__CONTAINER__SPMC_QUEUE_H

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

// A Single Producer Multiple Consumer queue
template <typename T, uint64_t SizeLog2>
class SPMCQueue final
{
public:
  class Receiver final
  {
  public:
    Receiver(
      std::shared_mutex& full_mutex,
      std::condition_variable_any& full_cond
    )
      : producer_size_(0),
      consumer_size_(0),
      start_(0),
      end_(0),
      full_mutex_(full_mutex),
      full_cond_(full_cond)
    {
      size_.store(0, std::memory_order_seq_cst);
    }

    ~Receiver() {
    }

    bool pop(T& value) {
      if (!optimistic_pop(value)) {
        std::unique_lock lock(empty_mutex_);
        empty_cond_.wait(lock, [this, &value]{ return optimistic_pop(value); });
      }
      return true;
    }

  private:
    friend class SPMCQueue<T, SizeLog2>;

    size_t producer_size_;
    size_t consumer_size_;
    size_t start_;
    size_t end_;
    std::atomic<size_t> size_;

    std::shared_mutex& full_mutex_;
    std::condition_variable_any& full_cond_;

    std::mutex empty_mutex_;
    std::condition_variable empty_cond_;

    std::array<T, (1ul<<SizeLog2)> data_;

    inline bool optimistic_pop(T& value) {
      if (consumer_size_ == 0) {
        consumer_size_ = size_.load(std::memory_order_acquire);
      }

      if (consumer_size_ != 0) {
        std::shared_lock lock(full_mutex_);
        std::swap(data_[start_], value);
        size_.fetch_sub(1, std::memory_order_relaxed);
        lock.unlock();
        full_cond_.notify_one();
        start_ = (start_+1) & ((1<<SizeLog2)-1);
        consumer_size_ -= 1;
        return true;
      }
      return false;
    }
  };

  typedef std::shared_ptr<Receiver> ReceiverRef;

  SPMCQueue()
    : current_receiver_(0)
  {
  }

  ~SPMCQueue() {
  }

  SPMCQueue(const SPMCQueue&) = delete;
  SPMCQueue(SPMCQueue&&) = delete;

  ReceiverRef new_receiver() {
    auto receiver = std::make_shared<Receiver>(
      full_mutex_,
      full_cond_
    );
    receivers_.push_back(receiver);
    return receiver;
  }

  void push(T&& value) {
    if (!optimistic_push(value)) {
      std::unique_lock lock(full_mutex_);
      full_cond_.wait(lock, [this, &value]{ return optimistic_push(value); });
    }
  }

private:
  uint32_t current_receiver_;
  std::vector<ReceiverRef> receivers_;
  std::queue<T> queue_;
  std::shared_mutex full_mutex_;
  std::condition_variable_any full_cond_;

  inline bool optimistic_push(T& value) {
    for (size_t i = receivers_.size(); i; --i) {
      current_receiver_ = (current_receiver_+1) % receivers_.size();
      auto receiver = receivers_[current_receiver_].get();
      if (receiver->producer_size_ == (1ul<<SizeLog2)) {
        receiver->producer_size_ = receiver->size_
          .load(std::memory_order_acquire);
      }

      if (receiver->producer_size_ != (1ul<<SizeLog2)) {
        std::unique_lock lock(receiver->empty_mutex_);
        std::swap(receiver->data_[receiver->end_], value);
        receiver->size_.fetch_add(1, std::memory_order_relaxed);
        lock.unlock();
        receiver->empty_cond_.notify_one();
        receiver->end_ = (receiver->end_+1) & ((1<<SizeLog2)-1);
        receiver->producer_size_ += 1;
        return true;
      }
    }

    return false;
  }

};

} /* container */
} /* jmutils */

#endif
