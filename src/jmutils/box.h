#ifndef JMUTILS__BOX_H
#define JMUTILS__BOX_H

#include <atomic>

namespace jmutils
{
  template <typename T, typename C>
  class Box final
  {
  public:
    template<typename... Args>
    Box(Args&&... args) noexcept : value_(args...) {
      refcount_.store(1);
    }

    ~Box() noexcept {
    }

    Box(const Box& rhs) = delete;
    Box(Box&& rhs) = delete;

    Box& operator=(const Box& o) = delete;
    Box& operator=(Box&& o) = delete;

    inline T* get() {
      return &value_;
    }

    inline void increment_refcount() {
      refcount_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void decrement_refcount() {
      if (refcount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete this;
      }
    }

  private:
    std::atomic<C> refcount_;
    T value_;
  };
} /* jmutils */

#endif
