#ifndef MHCONFIG__API__REQUEST__REQUEST_H
#define MHCONFIG__API__REQUEST__REQUEST_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/metrics.h"
#include "jmutils/time.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace request
{

typedef mhconfig::proto::MHConfig::WithRawMethod_Get<
        mhconfig::proto::MHConfig::WithAsyncMethod_Update<
        mhconfig::proto::MHConfig::WithAsyncMethod_RunGC<
          mhconfig::proto::MHConfig::Service>>> CustomService;

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

class Request
{
public:
  Request(
      CustomService* service,
      grpc::ServerCompletionQueue* cq,
      Metrics& metrics
  );
  virtual ~Request();

  virtual const std::string name() const = 0;

  virtual Request* clone() = 0;
  virtual void subscribe() = 0;

  void proceed();
  void reply();

protected:
  CustomService* service_;
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
  jmutils::time::MonotonicTimePoint start_time_;

  const std::string status();

};

} /* request */
} /* api */
} /* mhconfig */

#endif
