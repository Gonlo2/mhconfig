#ifndef MHCONFIG__API__SESSION_H
#define MHCONFIG__API__SESSION_H

#include <iostream>
#include <string>
#include <memory>

#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/command.h"

namespace mhconfig
{
namespace api
{

typedef mhconfig::proto::MHConfig::WithRawMethod_Get<
        mhconfig::proto::MHConfig::WithAsyncMethod_Update<
        mhconfig::proto::MHConfig::WithAsyncMethod_RunGC<
        mhconfig::proto::MHConfig::WithRawMethod_Watch<
        mhconfig::proto::MHConfig::WithAsyncMethod_Trace<
          mhconfig::proto::MHConfig::Service>>>>> CustomService;

template <typename T>
std::vector<T> to_vector(const ::google::protobuf::RepeatedPtrField<T>& proto_repeated) {
  std::vector<T> result;
  result.reserve(proto_repeated.size());
  result.insert(result.begin(), proto_repeated.cbegin(), proto_repeated.cend());
  return result;
}

template <typename T>
void delete_object(T* o) {
  o->~T();
  free(o);
}

template <typename T, typename... Args>
inline std::shared_ptr<T> make_session(Args&&... args)
{
  // This allow use the last 3 bits to store the status of the event
  void* data = aligned_alloc(8, sizeof(T));
  assert (data != nullptr);
  T* ptr = new (data) T(std::forward<Args>(args)...);
  auto session = std::shared_ptr<T>(ptr, delete_object<T>);
  session->init(session);
  return session;
}

class Session
{
public:
  Session() {
  }

  virtual ~Session() {
  }

  // This allow destroy the class when the service finished and
  // all the messages are destroyed
  void init(std::shared_ptr<Session> this_shared) {
    this_shared_ = std::move(this_shared);
    if (auto t = raw_tag(7)) ctx_.AsyncNotifyWhenDone(t);
  }

  virtual const std::string name() const = 0;

  //TODO move this functions to the protected sections and make the
  //class Service friend of this
  virtual void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) = 0;
  virtual void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) = 0;

  //TODO move this function to the private sections and make the
  //class Service friend of this
  std::shared_ptr<Session> proceed(
    uint8_t status,
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics,
    uint_fast32_t& sequential_id
  );

  //TODO move this function to the private sections and make the
  //class Service friend of this
  std::shared_ptr<Session> error(
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics
  );

  std::string session_peer() const {
    return ctx_.peer();
  }

protected:
  CustomService* service_;
  grpc::ServerCompletionQueue* cq_;

private:
  std::shared_ptr<Session> this_shared_{nullptr};
  absl::Mutex mutex_;
  uint32_t cq_refcount_{0};
  bool closed_{false};

  inline std::shared_ptr<Session> decrement_cq_refcount(
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics
  ) {
    std::shared_ptr<Session> tmp{nullptr};
    if (--cq_refcount_ == 0) {
      closed_ = true;
      tmp.swap(this_shared_);
      spdlog::trace("Destroying the gRPC event {}", (void*) this);
      on_destroy(scheduler_sender, metrics);
    }
    return tmp;
  }

protected:
  grpc::ServerContext ctx_;

  inline void* raw_tag(uint8_t status) {
    uintptr_t ptr = 0;
    mutex_.Lock();
    if (!closed_) {
      ++cq_refcount_;
      ptr = ((uintptr_t) this_shared_.get()) | status;
    }
    mutex_.Unlock();
    return (void*) ptr;
  }

  virtual void on_proceed(
    uint8_t status,
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics,
    uint_fast32_t& sequential_id
  ) = 0;

  virtual void on_destroy(
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics
  ) {
  };

};

} /* api */
} /* mhconfig */

#endif
