#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/command.h"
#include "mhconfig/validator.h"
#include "mhconfig/scheduler/api_get_command.h"

#include <grpcpp/impl/codegen/serialization_traits.h>

namespace mhconfig
{
namespace api
{
namespace request
{

class GetRequestImpl : public Request, public GetRequest, public std::enable_shared_from_this<GetRequestImpl>
{
public:
  GetRequestImpl();
  virtual ~GetRequestImpl();

  const std::string name() const override;

  void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

  const std::string& root_path() const override;
  uint32_t version() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& flavors() const override;
  const std::string& document() const override;

  void set_status(Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(const mhconfig::Element& element) override;
  void set_element_bytes(const char* data, size_t len) override;

  bool commit() override;

  bool finish(const grpc::Status& status = grpc::Status::OK) override;

  std::string peer() const override;

protected:
  google::protobuf::Arena arena_;
  grpc::ServerAsyncResponseWriter<grpc::ByteBuffer> responder_;

  grpc::ByteBuffer raw_request_;

  mhconfig::proto::GetRequest* request_;
  mhconfig::proto::GetResponse* response_;

  std::stringstream elements_data_;

  std::vector<std::string> overrides_;
  std::vector<std::string> flavors_;

  Element element_;

  void request(
    auth::Acl* acl,
    SchedulerQueue::Sender* scheduler_sender
  ) override;

};

} /* request */
} /* api */
} /* mhconfig */

#endif
