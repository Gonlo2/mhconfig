#ifndef JMUTILS__ALIGNED_PTR_H
#define JMUTILS__ALIGNED_PTR_H

#include <cstdint>
#include <utility>
#include <stdlib.h>
#include <assert.h>

namespace jmutils
{

template <uintptr_t Align, typename Ptr, typename Value>
class AlignedPtr final
{
public:
    explicit AlignedPtr() noexcept : payload_(0) {
    }

    explicit AlignedPtr(uintptr_t payload) noexcept : payload_(payload) {
    }

    inline void set(Ptr* ptr, Value value) {
        payload_ = (uintptr_t) ptr | (uintptr_t) value;
    }

    inline Ptr* ptr() const {
        return (Ptr*) (payload_ & ~((1ull<<Align)-1));
    }

    inline void ptr(Ptr* ptr) {
        payload_ = (uintptr_t) ptr | (payload_ & ((1ull<<Align)-1));
    }

    inline Value value() const {
        return (Value) (payload_ & ((1ull<<Align)-1));
    }

    inline void value(Value value) {
        payload_ = (payload_ & ~((1ull<<Align)-1)) | (uintptr_t) value;
    }

    inline void incr() {
        ++payload_;
    }

    inline void decr() {
        --payload_;
    }

    inline void destroy() {
        Ptr* p = ptr();
        p->~Ptr();
        free(p);
    }

private:
    uintptr_t payload_;
};

template <uintptr_t Align, typename Ptr, typename Value, typename... Args>
AlignedPtr<Align, Ptr, Value> new_aligned_ptr(Value value, Args&&... args) noexcept {
    void* data = aligned_alloc(1ull<<Align, sizeof(Ptr));
    assert(data != nullptr);
    new (data) Ptr(std::forward<Args>(args)...);
    return AlignedPtr<Align, Ptr, Value>((uintptr_t) data | value);
}

} /* jmutils */

#endif
