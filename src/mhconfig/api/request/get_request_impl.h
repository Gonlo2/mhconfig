#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/api_get_command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

using jmutils::container::Queue;


class GetRequestImpl : public Request, public GetRequest, public std::enable_shared_from_this<GetRequestImpl>
{
public:
  GetRequestImpl(
      CustomService* service,
      grpc::ServerCompletionQueue* cq_,
      Metrics& metrics,
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~GetRequestImpl();

  const std::string name() const override;

  std::shared_ptr<Session> clone() override;
  void subscribe() override;

  const std::string& root_path() const override;
  const uint32_t version() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& key() const override;

  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(mhconfig::ElementRef element) override;
  void set_element_bytes(const char* data, size_t len) override;

  bool commit() override;

protected:
  grpc::ServerAsyncResponseWriter<grpc::ByteBuffer> responder_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  grpc::ByteBuffer raw_request_;

  mhconfig::proto::GetRequest request_;
  mhconfig::proto::GetResponse response_;

  std::stringstream elements_data_;

  std::vector<std::string> overrides_;
  std::vector<std::string> key_;

  mhconfig::ElementRef element_{nullptr};

  void request() override;
  void finish() override;
};

} /* request */
} /* api */
} /* mhconfig */

#endif
