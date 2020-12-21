#ifndef MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H

#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpcpp/impl/codegen/async_unary_call_impl.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/status.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/session.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/proto/mhconfig.pb.h"
#include "mhconfig/validator.h"
#include "mhconfig/worker/setup_command.h"
#include "mhconfig/worker/update_command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class UpdateRequestImpl final
  : public Request,
  public UpdateRequest,
  public PolicyCheck,
  public std::enable_shared_from_this<UpdateRequestImpl>
{
public:
  template <typename T>
  UpdateRequestImpl(
    T&& ctx
  ) : Request(std::forward<T>(ctx)),
    responder_(&server_ctx_)
  {
    request_ = Arena::CreateMessage<mhconfig::proto::UpdateRequest>(&arena_);
    response_ = Arena::CreateMessage<mhconfig::proto::UpdateResponse>(&arena_);
  }

  ~UpdateRequestImpl();

  const std::string& root_path() const override;
  const std::vector<std::string>& relative_paths() const override;
  bool reload() const override;

  void set_namespace_id(uint64_t namespace_id) override;
  void set_status(Status status) override;
  void set_version(uint32_t version) override;

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
  grpc::ServerAsyncResponseWriter<mhconfig::proto::UpdateResponse> responder_;

  mhconfig::proto::UpdateRequest* request_;
  mhconfig::proto::UpdateResponse* response_;

  std::vector<std::string> relative_paths_;

  std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  std::shared_ptr<PolicyCheck> parse_message() override;

private:
  bool validate_request();
};

} /* request */
} /* api */
} /* mhconfig */

#endif
