#ifndef MHCONFIG__API__STREAM__TRACE_STREAM_IMPL_H
#define MHCONFIG__API__STREAM__TRACE_STREAM_IMPL_H

#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpcpp/impl/codegen/async_stream_impl.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <memory>
#include <string>
#include <utility>

#include "jmutils/container/label_set.h"
#include "mhconfig/api/common.h"
#include "mhconfig/api/session.h"
#include "mhconfig/api/stream/stream.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/proto/mhconfig.pb.h"
#include "mhconfig/provider.h"
#include "mhconfig/validator.h"
#include "mhconfig/worker/setup_command.h"
#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

class TraceStreamImpl;

class TraceOutputMessageImpl final
  : public TraceOutputMessage,
  public std::enable_shared_from_this<TraceOutputMessageImpl>
{
public:
  TraceOutputMessageImpl(
    std::weak_ptr<TraceStreamImpl>&& stream
  );

  void set_status(Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_labels(const Labels& labels) override;
  void set_document(const std::string& document) override;

  bool send(bool finish = false) override;

protected:
  friend class Stream<grpc::ServerAsyncWriter<mhconfig::proto::TraceResponse>, TraceOutputMessageImpl>;

  inline const mhconfig::proto::TraceResponse& response() {
    return *response_;
  }

private:
  google::protobuf::Arena arena_;
  mhconfig::proto::TraceResponse* response_;
  std::weak_ptr<TraceStreamImpl> stream_;
};

class TraceStreamImpl final
  : public Stream<grpc::ServerAsyncWriter<mhconfig::proto::TraceResponse>, TraceOutputMessageImpl>,
  public TraceInputMessage,
  public PolicyCheck,
  public std::enable_shared_from_this<TraceStreamImpl>
{
public:
  template <typename T>
  TraceStreamImpl(
    T&& request
  ) : Stream(std::forward<T>(request)) {
    request_ = Arena::CreateMessage<mhconfig::proto::TraceRequest>(&arena_);
  };

  const std::string& root_path() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  std::shared_ptr<TraceOutputMessage> make_output_message() override;

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
  friend class TraceOutputMessageImpl;

  std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  std::shared_ptr<PolicyCheck> parse_message() override;

private:
  google::protobuf::Arena arena_;
  mhconfig::proto::TraceRequest *request_;

  Labels labels_;
};

} /* stream */
} /* api */
} /* mhconfig */

#endif
