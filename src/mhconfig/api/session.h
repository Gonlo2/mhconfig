#ifndef MHCONFIG__API__SESSION_H
#define MHCONFIG__API__SESSION_H

#include <iostream>
#include <string>

#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/api/session.h"

namespace mhconfig
{
namespace api
{

typedef mhconfig::proto::MHConfig::WithRawMethod_Get<
        mhconfig::proto::MHConfig::WithAsyncMethod_Update<
        mhconfig::proto::MHConfig::WithAsyncMethod_RunGC<
        mhconfig::proto::MHConfig::WithRawMethod_Watch<
          mhconfig::proto::MHConfig::Service>>>> CustomService;

template <typename T>
std::vector<T> to_vector(const ::google::protobuf::RepeatedPtrField<T>& proto_repeated) {
  std::vector<T> result;
  result.reserve(proto_repeated.size());
  result.insert(result.begin(), proto_repeated.cbegin(), proto_repeated.cend());
  return result;
}

bool parse_from_byte_buffer(
  const grpc::ByteBuffer& buffer,
  grpc::protobuf::Message& message
);

class Commitable
{
public:
  Commitable() {
  }
  virtual ~Commitable() {
  }

  virtual bool commit() = 0;
};

template<typename T, typename... Args>
inline std::shared_ptr<T> make_session(Args&&... args)
{
  auto session = std::make_shared<T>(args...);
  session->set_this_shared(session);
  return session;
}

class Session
{
public:
  Session(
      CustomService* service,
      grpc::ServerCompletionQueue* cq
  ) :
      service_(service),
      cq_(cq)
  {
  }

  virtual ~Session() {
  }

  // This allow destroy the class when the service finished and
  // all the messages are destroyed
  void set_this_shared(
    std::shared_ptr<Session> this_shared
  ) {
    std::lock_guard<std::recursive_mutex> mlock(mutex_);
    this_shared_ = this_shared;
  }

  virtual const std::string name() const = 0;

  //TODO move this functions to the protected sections and make the
  //class Service friend of this
  virtual std::shared_ptr<Session> clone() = 0;
  virtual void subscribe() = 0;

  //TODO move this function to the private sections and make the
  //class Service friend of this
  virtual std::shared_ptr<Session> proceed() = 0;

  inline void* tag() {
    return (this_shared_ == nullptr) ? 0 : this_shared_.get();
  }

  inline bool is_destroyed() {
    return this_shared_ == nullptr;
  }

  // Once the recursive smart pointer is destroyed the class will be
  // destroyed when the messages are also removed
  virtual std::shared_ptr<Session> destroy() {
    std::lock_guard<std::recursive_mutex> mlock(mutex_);
    std::shared_ptr<Session> tmp{nullptr};
    tmp.swap(this_shared_);
    return tmp;
  }

protected:
  CustomService* service_;
  grpc::ServerCompletionQueue* cq_;

private:
  std::shared_ptr<Session> this_shared_{nullptr};

protected:
  std::recursive_mutex mutex_;
  grpc::ServerContext ctx_;
};

} /* api */
} /* mhconfig */

#endif
