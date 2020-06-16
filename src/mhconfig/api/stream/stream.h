#ifndef MHCONFIG__API__STREAM__STREAM_H
#define MHCONFIG__API__STREAM__STREAM_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <queue>

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

template <typename Req, typename Resp, typename OutMsg>
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
  std::shared_ptr<Session> proceed(
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService& metrics,
    uint_fast32_t& sequential_id
  ) override {
    std::lock_guard<std::recursive_mutex> mlock(mutex_);
    if (is_destroyed()) {
      spdlog::debug("Isn't possible call to proceed in a destroyed request");
    } else {
      spdlog::debug("Received gRPC event {} in {} status", name(), status());

      if (status_ == Status::CREATE) {
        status_ = Status::PROCESS;

        clone_and_subscribe(service, cq);

        //TODO add request metrics
        prepare_next_request();
      } else if (status_ == Status::PROCESS) {
        request(scheduler_sender);
        prepare_next_request();
        send_message_if_neccesary();
      } else if (status_ == Status::WRITING) {
        status_ = Status::PROCESS;
        send_message_if_neccesary();
      } else if (status_ == Status::FINISH) {
        return destroy();
      } else {
        assert(false);
      }
    }
    return nullptr;
  }

  std::shared_ptr<Session> destroy() override final {
    std::lock_guard<std::recursive_mutex> mlock(mutex_);
    ctx_.TryCancel();  //TODO This is the correct way to handle this?
    while (!messages_to_send_.empty()) {
      messages_to_send_.pop();
    }
    return Session::destroy();
  }

protected:
  grpc::ServerAsyncReaderWriter<Resp, Req> stream_;

  //TODO Use this with CRTP
  virtual void prepare_next_request() = 0;

  //TODO Use this with CRTP
  virtual void request(
    SchedulerQueue::Sender* scheduler_sender
  ) = 0;

  bool send(std::shared_ptr<OutMsg> message, bool finish) {
    std::lock_guard<std::recursive_mutex> mlock(mutex_);
    if (going_to_finish_ || is_destroyed()) {
      return false;
    }

    going_to_finish_ = finish;
    messages_to_send_.push(message);

    switch (status_) {
      case Status::CREATE:
        assert(false); // If we are here we have a problem
      case Status::PROCESS:
        send_message_if_neccesary();
        return true;
      case Status::WRITING:
        return true;
      case Status::FINISH:
        return false;
    }

    assert(false); // If we are here we have a problem
  }

private:
  enum class Status {
    CREATE,
    PROCESS,
    WRITING,
    FINISH
  };

  Status status_{Status::CREATE};

  std::queue<std::shared_ptr<OutMsg>> messages_to_send_;
  bool going_to_finish_{false};

  void send_message_if_neccesary() {
    if (!messages_to_send_.empty()) {
      auto message = messages_to_send_.front();
      messages_to_send_.pop();
      bool finish = going_to_finish_ && messages_to_send_.empty();
      if (finish) {
        stream_.WriteAndFinish(
          message->response_,
          grpc::WriteOptions(),
          grpc::Status::OK,
          tag()
        );
      } else {
        stream_.Write(
          message->response_,
          tag()
        );
      }
      status_ = finish ? Status::FINISH : Status::WRITING;
    }
  }

  const std::string status() {
    switch (status_) {
      case Status::CREATE: return "CREATE";
      case Status::PROCESS: return "PROCESS";
      case Status::WRITING: return "WRITING";
      case Status::FINISH: return "FINISH";
    }

    return "unknown";
  }

};

} /* stream */
} /* api */
} /* mhconfig */

#endif
