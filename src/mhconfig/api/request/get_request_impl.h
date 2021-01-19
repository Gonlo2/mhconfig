#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_IMPL_H

#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpcpp/impl/codegen/async_unary_call_impl.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/serialization_traits.h>
#include <grpcpp/impl/codegen/status.h>
#include <stddef.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "jmutils/container/label_set.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/api/session.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/element.h"
#include "mhconfig/proto/mhconfig.pb.h"
#include "mhconfig/provider.h"
#include "mhconfig/validator.h"
#include "mhconfig/worker/setup_command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class GetRequestImpl final
  : public Request,
  public GetRequest,
  public PolicyCheck,
  public std::enable_shared_from_this<GetRequestImpl>
{
public:
  template <typename T>
  GetRequestImpl(
    T&& ctx
  ) : Request(std::forward<T>(ctx)),
    responder_(&server_ctx_)
  {
    request_ = Arena::CreateMessage<mhconfig::proto::GetRequest>(&arena_);
    response_ = Arena::CreateMessage<mhconfig::proto::GetResponse>(&arena_);
  }

  ~GetRequestImpl();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  void set_status(Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(const mhconfig::Element& element) override;
  void set_checksum(const uint8_t* data, size_t len) override;

  bool commit() override;
  bool finish(const grpc::Status& status = grpc::Status::OK) override;

  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

  void on_check_policy(
    auth::AuthResult auth_result,
    auth::Policy* policy
  ) override;

  void on_check_policy_error() override;

protected:
  google::protobuf::Arena arena_;
  grpc::ServerAsyncResponseWriter<mhconfig::proto::GetResponse> responder_;

  mhconfig::proto::GetRequest* request_;
  mhconfig::proto::GetResponse* response_;

  Labels labels_;
  Element element_;

  std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  std::shared_ptr<PolicyCheck> parse_message() override;

};

} /* request */
} /* api */
} /* mhconfig */

#endif
