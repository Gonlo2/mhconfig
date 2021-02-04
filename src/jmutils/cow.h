#ifndef JMUTILS__COW_H
#define JMUTILS__COW_H

#include <atomic>
#include <utility>

namespace jmutils
{

template <typename T>
class Cow final
{
public:
  template <typename... Args>
  explicit Cow(Args&&... args) noexcept {
    payload_ = new payload_t(std::forward<Args>(args)...);
    assert(payload_ != nullptr);
  }

  ~Cow() noexcept {
    decrement_refcount();
  }

  Cow(const Cow& rhs) noexcept : payload_(rhs.payload_) {
    increment_refcount();
  }

  Cow(Cow&& rhs) noexcept : payload_(std::exchange(rhs.payload_, nullptr)) {
  }

  Cow& operator=(const Cow& o) noexcept {
    if (this != &o) {
      decrement_refcount();
      payload_ = o.payload_;
      increment_refcount();
    }
    return *this;
  }

  Cow& operator=(Cow&& o) noexcept {
    std::swap(payload_, o.payload_);
    return *this;
  }

  template <typename V>
  inline bool set(V&& v) {
    T* value = get_mut();
    if (value == nullptr) return false;
    *value = std::forward<V>(v);
    return true;
  }

  inline const T* get() const {
    return payload_ == nullptr ? nullptr : &payload_->value;
  }

  inline T* get_mut() {
    if ((payload_ != nullptr) && payload_->frozen) {
      auto new_payload = new payload_t(payload_->value);
      decrement_refcount();
      payload_ = new_payload;
    }
    return payload_ == nullptr ? nullptr : &payload_->value;
  }

  bool is_frozen() const {
    return (payload_ == nullptr) || payload_->frozen;
  }

  template <typename L>
  void freeze(L lambda) {
    if (payload_ != nullptr) {
      if (!payload_->frozen) {
        lambda(&payload_->value);
      }
      payload_->frozen = true;
    }
  }

private:
  struct payload_t {
    mutable std::atomic<uint32_t> refcount;
    bool frozen{false};
    T value;

    template <typename... Args>
    payload_t(Args&&... args) noexcept : value(std::forward<Args>(args)...) {
      refcount.store(1);
    }
  };

  payload_t* payload_;

  inline void increment_refcount() {
    if (payload_ != nullptr) {
      payload_->refcount.fetch_add(1, std::memory_order_relaxed);
    }
  }

  inline void decrement_refcount() {
    if (payload_ != nullptr) {
      if (payload_->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete payload_;
        payload_ = nullptr;
      }
    }
  }
};

} /* jmutils */

#endif
