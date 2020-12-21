#ifndef MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__RUN_GC_REQUEST_IMPL_H

#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpcpp/impl/codegen/async_unary_call_impl.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/status.h>
#include <memory>
#include <utility>

#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/run_gc_request.h"
#include "mhconfig/api/session.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/proto/mhconfig.pb.h"
#include "mhconfig/worker/gc_config_namespaces_command.h"
#include "mhconfig/worker/gc_dead_pointers_command.h"
#include "mhconfig/worker/gc_merged_configs_command.h"
#include "mhconfig/worker/gc_raw_config_versions_command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class RunGCRequestImpl final
  : public Request,
  public RunGCRequest,
  public PolicyCheck,
  public std::enable_shared_from_this<RunGCRequestImpl>
{
public:
  template <typename T>
  RunGCRequestImpl(
    T&& request
  ) : Request(std::forward<T>(request)),
    responder_(&server_ctx_)
  {
    request_ = Arena::CreateMessage<mhconfig::proto::RunGCRequest>(&arena_);
    response_ = Arena::CreateMessage<mhconfig::proto::RunGCResponse>(&arena_);
  }

  ~RunGCRequestImpl();

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
  grpc::ServerAsyncResponseWriter<mhconfig::proto::RunGCResponse> responder_;

  mhconfig::proto::RunGCRequest* request_;
  mhconfig::proto::RunGCResponse* response_;

  std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  std::shared_ptr<PolicyCheck> parse_message() override;

private:
  uint32_t max_live_in_seconds();

  std::unique_ptr<WorkerCommand> make_gc_command();
};

} /* request */
} /* api */
} /* mhconfig */

#endif
