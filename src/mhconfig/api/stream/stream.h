#ifndef MHCONFIG__API__STREAM__STREAM_H
#define MHCONFIG__API__STREAM__STREAM_H

#include <absl/synchronization/mutex.h>
#include <grpcpp/impl/codegen/call_op_set.h>
#include <grpcpp/impl/codegen/status.h>
#include <chrono>
#include <deque>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <utility>

#include "jmutils/time.h"
#include "mhconfig/api/session.h"
#include "mhconfig/metrics.h"
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
  template <typename T>
  Stream(
    T&& ctx
  ) : Session(std::forward<T>(ctx)),
    stream_(&server_ctx_)
  {
  }

  virtual ~Stream() {
  }

  bool finish(const grpc::Status& status) override {
    mutex_.Lock();
    bool ok = !going_to_finish_;
    if (ok) {
      going_to_finish_ = true;
      finish_status_ = status;
      send_message_if_neccesary();
    }
    mutex_.Unlock();
    return ok;
  }

protected:
  GrpcStream stream_;

  void on_write() override {
    mutex_.Lock();
    sending_a_message_ = false;
    send_message_if_neccesary();
    mutex_.Unlock();
  }

  bool send(
    std::shared_ptr<OutMsg> message,
    bool finish
  ) {
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
  bool going_to_finish_{false};
  bool sending_a_message_{false};
  grpc::Status finish_status_{grpc::Status::OK};

  void send_message_if_neccesary() {
    if (!sending_a_message_) {
      if (!messages_to_send_.empty()) {
        if (auto t = make_tag_locked(GrpcStatus::WRITE)) {
          spdlog::trace("Sending a message in the stream {}", (void*) this);
          auto message = messages_to_send_.front();
          messages_to_send_.pop_front();
          bool finish = going_to_finish_ && messages_to_send_.empty();
          if (finish) {
            stream_.WriteAndFinish(
              message->response(),
              grpc::WriteOptions(),
              finish_status_,
              t
            );
            going_to_finish_ = false;
          } else {
            stream_.Write(message->response(), t);
          }
          sending_a_message_ = true;
        }
      } else if (going_to_finish_) {
        if (auto t = make_tag_locked(GrpcStatus::WRITE)) {
          stream_.Finish(finish_status_, t);
          going_to_finish_ = false;
        }
      }
    }
  }

};

} /* stream */
} /* api */
} /* mhconfig */

#endif
