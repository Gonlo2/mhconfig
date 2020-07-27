#ifndef MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H
#define MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/stream.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/command.h"
#include "mhconfig/validator.h"
#include "mhconfig/scheduler/api_watch_command.h"
#include "mhconfig/scheduler/on_watchers_removed_command.h"
#include "mhconfig/scheduler/common.h"

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <grpcpp/impl/codegen/serialization_traits.h>

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

class WatchStreamImpl;
class WatchInputMessageImpl;

class WatchOutputMessageImpl : public WatchOutputMessage, public std::enable_shared_from_this<WatchOutputMessageImpl>
{
public:
  WatchOutputMessageImpl(
    std::weak_ptr<WatchStreamImpl>& stream
  );
  virtual ~WatchOutputMessageImpl();

  void set_uid(uint32_t uid) override;
  void set_status(WatchStatus status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(const mhconfig::Element& element) override;
  void set_element_bytes(const char* data, size_t len) override;
  void set_template_rendered(const std::string& data) override;

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
  std::stringstream elements_data_;

  grpc::Slice slice_;
};

class WatchInputMessageImpl : public WatchInputMessage
{
public:
  WatchInputMessageImpl(
    std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
    std::weak_ptr<WatchStreamImpl>&& stream
  );
  virtual ~WatchInputMessageImpl();

  uint32_t uid() const override;
  bool remove() const override;
  const std::string& root_path() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& flavors() const override;
  uint32_t version() const override;
  const std::string& document() const override;
  const std::string& template_() const override;

  bool unregister() override;

  std::string peer() const override;

  std::shared_ptr<WatchOutputMessage> make_output_message() override;

private:
  std::unique_ptr<mhconfig::proto::WatchRequest> request_;
  std::weak_ptr<WatchStreamImpl> stream_;

  std::vector<std::string> overrides_;
  std::vector<std::string> flavors_;
};

class WatchGetRequest : public ::mhconfig::api::request::GetRequest
{
public:
  WatchGetRequest(
    std::shared_ptr<WatchInputMessage> input_message,
    std::shared_ptr<WatchOutputMessage> output_message
  );
  virtual ~WatchGetRequest();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& flavors() const override;
  const std::string& document() const override;
  const std::string& template_() const override;

  void set_status(::mhconfig::api::request::GetRequest::Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(const mhconfig::Element& element) override;
  void set_element_bytes(const char* data, size_t len) override;
  void set_template_rendered(const std::string& data) override;

  bool commit() override;

  std::string peer() const override;

private:
  std::shared_ptr<WatchInputMessage> input_message_;
  std::shared_ptr<WatchOutputMessage> output_message_;
};

class WatchStreamImpl final
  : public Stream<grpc::ServerAsyncReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>, WatchOutputMessageImpl>,
    public std::enable_shared_from_this<WatchStreamImpl>
{
public:
  WatchStreamImpl();
  virtual ~WatchStreamImpl();

  const std::string name() const override;

  void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

  bool unregister(uint32_t uid);

protected:
  friend class WatchOutputMessageImpl;

  void on_create(
    auth::Acl* acl,
    SchedulerQueue::Sender* scheduler_sender
  ) override;

  void on_read(
    auth::Acl* acl,
    SchedulerQueue::Sender* scheduler_sender
  ) override;

  void on_destroy(
    SchedulerQueue::Sender* scheduler_sender,
    metrics::MetricsService* metrics
  ) override;

private:
  grpc::ByteBuffer next_req_;
  absl::flat_hash_map<uint32_t, std::shared_ptr<WatchInputMessage>> watcher_by_id_;
  SchedulerQueue::Sender* scheduler_sender_{nullptr};
  absl::Mutex mutex_;

  bool unregister(SchedulerQueue::Sender* scheduler_sender, uint32_t uid);

  inline void prepare_next_request() {
    if (auto t = tag(Status::READ)) {
      next_req_.Clear();
      stream_.Read(&next_req_, t);
    }
  }

};


} /* stream */
} /* api */
} /* mhconfig */

#endif
