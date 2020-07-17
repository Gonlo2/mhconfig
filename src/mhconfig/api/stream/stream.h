#ifndef MHCONFIG__API__STREAM__STREAM_H
#define MHCONFIG__API__STREAM__STREAM_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <queue>

#include <absl/synchronization/mutex.h>

#include "mhconfig/api/session.h"
#include "mhconfig/metrics/metrics_service.h"
#include "jmutils/time.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

template <typename GrpcStream, typename OutMsg>
class Stream : public Session
{
public:
  Stream()
    : Session(),
    stream_(&ctx_)
  {
  }

  virtual ~Stream() {
  }

  //TODO move this function to the private sections and make the
  //class Service friend of this
  void on_proceed(
    uint8_t status,
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics,
    uint_fast32_t& sequential_id
  ) override {
    switch (static_cast<Status>(status)) {
      case Status::CREATE:
        clone_and_subscribe(service, cq);
        //TODO add request metrics
        on_create(scheduler_sender);
        break;
      case Status::READ:
        on_read(scheduler_sender);
        break;
      case Status::WRITE:
        mutex_.Lock();
        sending_a_message_ = false;
        send_message_if_neccesary();
        mutex_.Unlock();
        break;
      case Status::FINISH:
        break;
    }
  }

protected:
  enum class Status {
    CREATE = 0,
    READ = 1,
    WRITE = 2,
    FINISH = 7
  };

  GrpcStream stream_;

  inline void* tag(Status status) {
    return raw_tag(static_cast<uint8_t>(status));
  }

  //TODO Use this with CRTP
  virtual void on_create(
    SchedulerQueue::Sender* scheduler_sender
  ) = 0;

  //TODO Use this with CRTP
  virtual void on_read(
    SchedulerQueue::Sender* scheduler_sender
  ) = 0;

  bool send(std::shared_ptr<OutMsg> message, bool finish) {
    mutex_.Lock();
    bool ok = !going_to_finish_;
    if (ok) {
      going_to_finish_ = finish;
      messages_to_send_.push_back(message);
      send_message_if_neccesary();
    }
    mutex_.Unlock();
    return ok;
  }

private:
  std::deque<std::shared_ptr<OutMsg>> messages_to_send_;
  absl::Mutex mutex_;
  bool going_to_finish_{false};
  bool sending_a_message_{false};

  void send_message_if_neccesary() {
    if (!sending_a_message_ && !messages_to_send_.empty()) {
      if (auto t = tag(Status::WRITE)) {
        spdlog::trace("Sending a message in the stream {}", (void*) this);
        auto message = messages_to_send_.front();
        messages_to_send_.pop_front();
        bool finish = going_to_finish_ && messages_to_send_.empty();
        if (finish) {
          stream_.WriteAndFinish(
            message->response(),
            grpc::WriteOptions(),
            grpc::Status::OK,
            t
          );
        } else {
          stream_.Write(
            message->response(),
            t
          );
        }
        sending_a_message_ = true;
      }
    }
  }

};

} /* stream */
} /* api */
} /* mhconfig */

#endif
