#ifndef JMUTILS__CONTAINER__SPMC_QUEUE_H
#define JMUTILS__CONTAINER__SPMC_QUEUE_H

#include <stdlib.h>
#include <memory>
#include <queue>
#include <thread>
#include <utility>
#include <atomic>
#include <array>
#include <vector>

#include <absl/synchronization/mutex.h>

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
      absl::Mutex& full_mutex,
      absl::CondVar& full_cond
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
        empty_mutex_.Lock();
        while (!optimistic_pop(value)) {
          empty_cond_.Wait(&empty_mutex_);
        }
        empty_mutex_.Unlock();
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

    absl::Mutex& full_mutex_;
    absl::CondVar& full_cond_;

    absl::Mutex empty_mutex_;
    absl::CondVar empty_cond_;

    std::array<T, (1ul<<SizeLog2)> data_;

    inline bool optimistic_pop(T& value) {
      if (consumer_size_ == 0) {
        consumer_size_ = size_.load(std::memory_order_acquire);
      }

      if (consumer_size_ != 0) {
        full_mutex_.ReaderLock();
        value = std::move(data_[start_]);
        size_.fetch_sub(1, std::memory_order_relaxed);
        full_cond_.Signal();
        full_mutex_.ReaderUnlock();
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
    if (!optimistic_push<true>(value)) {
      full_mutex_.Lock();
      while (!optimistic_push<false>(value)) {
        full_cond_.Wait(&full_mutex_);
      }
      full_mutex_.Unlock();
    }
  }

private:
  uint32_t current_receiver_;
  std::vector<ReceiverRef> receivers_;
  std::queue<T> queue_;
  absl::Mutex full_mutex_;
  absl::CondVar full_cond_;

  template <bool lock>
  inline bool optimistic_push(T& value) {
    for (size_t i = receivers_.size(); i; --i) {
      current_receiver_ = (current_receiver_+1) % receivers_.size();
      auto receiver = receivers_[current_receiver_].get();
      if (receiver->producer_size_ == (1ul<<SizeLog2)) {
        receiver->producer_size_ = receiver->size_
          .load(std::memory_order_acquire);
      }

      if (receiver->producer_size_ != (1ul<<SizeLog2)) {
        if (lock) receiver->empty_mutex_.Lock();
        receiver->data_[receiver->end_] = std::move(value);
        receiver->size_.fetch_add(1, std::memory_order_relaxed);
        receiver->empty_cond_.Signal();
        if (lock) receiver->empty_mutex_.Unlock();
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
