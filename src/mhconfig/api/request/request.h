#ifndef MHCONFIG__API__REQUEST__REQUEST_H
#define MHCONFIG__API__REQUEST__REQUEST_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/metrics.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace request
{

template <typename T>
std::vector<T> to_vector(const ::google::protobuf::RepeatedPtrField<T>& proto_repeated) {
  std::vector<T> result;
  result.reserve(proto_repeated.size());
  result.insert(result.begin(), proto_repeated.cbegin(), proto_repeated.cend());
  return result;
}

class Request
{
public:
  Request(
      mhconfig::proto::MHConfig::AsyncService* service,
      grpc::ServerCompletionQueue* cq,
      Metrics& metrics
  );
  virtual ~Request();

  virtual const std::string name() const = 0;
  virtual const uint32_t id() const = 0;
  void reply();

  virtual Request* clone() = 0;
  virtual void subscribe() = 0;

  void proceed();

protected:
  mhconfig::proto::MHConfig::AsyncService* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerContext ctx_;

  Metrics& metrics_;

  virtual void request() = 0;
  virtual void finish() = 0;

private:
  enum Status {
    CREATE,
    PROCESS,
    FINISH
  };

  volatile Status status_{Status::CREATE};
  std::chrono::high_resolution_clock::time_point start_time_;

  const std::string status();

};

} /* request */
} /* api */
} /* mhconfig */

#endif
