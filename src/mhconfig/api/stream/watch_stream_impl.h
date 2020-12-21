#ifndef MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H
#define MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpcpp/impl/codegen/async_stream_impl.h>
#include <grpcpp/impl/codegen/byte_buffer.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/serialization_traits.h>
#include <grpcpp/impl/codegen/slice.h>
#include <stddef.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "jmutils/container/label_set.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/session.h"
#include "mhconfig/api/stream/stream.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/builder.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/validator.h"
#include "mhconfig/worker/setup_command.h"
#include "mhconfig/worker/update_command.h"
#include "spdlog/spdlog.h"

namespace mhconfig
{
//class Element;

namespace api
{
namespace stream
{

class WatchInputMessageImpl;
class WatchStreamImpl;

class WatchOutputMessageImpl final
  : public WatchOutputMessage,
  public std::enable_shared_from_this<WatchOutputMessageImpl>
{
public:
  WatchOutputMessageImpl(
    std::weak_ptr<WatchStreamImpl>& stream
  );
  ~WatchOutputMessageImpl();

  void set_uid(uint32_t uid) override;
  void set_status(WatchStatus status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(const mhconfig::Element& element) override;
  void set_checksum(const uint8_t* data, size_t len) override;

  void set_preprocessed_payload(const char* data, size_t len) override;

  bool send(bool finish = false) override;

protected:
  friend class Stream<grpc::ServerAsyncReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>, WatchOutputMessageImpl>;

  inline const grpc::ByteBuffer& response() {
    return response_;
  }

private:
  google::protobuf::Arena arena_;
  mhconfig::proto::WatchResponse* proto_response_;
  grpc::ByteBuffer response_;
  std::weak_ptr<WatchStreamImpl> stream_;
  std::stringstream preprocessed_payload_;

  grpc::Slice slice_;
};

class WatchInputMessageImpl final
  : public WatchInputMessage,
  public PolicyCheck,
  public std::enable_shared_from_this<WatchInputMessageImpl>
{
public:
  WatchInputMessageImpl(
    std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
    std::weak_ptr<WatchStreamImpl>&& stream
  );
  ~WatchInputMessageImpl();

  uint32_t uid() const override;
  bool remove() const override;
  const std::string& root_path() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  std::optional<std::optional<uint64_t>> unregister() override;

  std::shared_ptr<WatchOutputMessage> make_output_message() override;

  void on_check_policy(
    auth::AuthResult auth_result,
    auth::Policy* policy
  ) override;

  void on_check_policy_error() override;

private:
  std::unique_ptr<mhconfig::proto::WatchRequest> request_;
  std::weak_ptr<WatchStreamImpl> stream_;

  Labels labels_;

  bool check_auth(auth::AuthResult auth_result);

};

class WatchStreamImpl final
  : public Stream<grpc::ServerAsyncReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>, WatchOutputMessageImpl>,
  public PolicyCheck,
  public std::enable_shared_from_this<WatchStreamImpl>
{
public:
  template <typename T>
  WatchStreamImpl(
    T&& request
  ) : Stream(std::forward<T>(request)) {
  };
  ~WatchStreamImpl();

  void register_(
    std::shared_ptr<WatchInputMessage>&& msg
  );

  std::optional<uint64_t> unregister(uint32_t uid);

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
  friend class WatchOutputMessageImpl;

  std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  std::shared_ptr<PolicyCheck> parse_message() override;
  void on_destroy() override;

private:
  grpc::ByteBuffer next_req_;
  absl::flat_hash_map<uint32_t, std::shared_ptr<WatchInputMessage>> watcher_by_id_;
  absl::Mutex mutex_;

  inline void prepare_next_request() {
    if (auto t = make_tag(GrpcStatus::READ)) {
      next_req_.Clear();
      stream_.Read(&next_req_, t);
    }
  }

  void on_removed_watcher(
    config_namespace_t* cn,
    const WatchInputMessage* request
  );

};


} /* stream */
} /* api */
} /* mhconfig */

#endif
