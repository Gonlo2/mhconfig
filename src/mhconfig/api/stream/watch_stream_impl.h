#ifndef MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H
#define MHCONFIG__API__STREAM__WATCH_STREAM_IMPL_H

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/api_watch_command.h"

#include "jmutils/container/queue.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

using jmutils::container::Queue;

class WatchStreamImpl;
class WatchInputMessageImpl;

class WatchOutputMessageImpl : public WatchOutputMessage, public std::enable_shared_from_this<WatchOutputMessageImpl>
{
public:
  WatchOutputMessageImpl(
    std::shared_ptr<WatchStreamImpl> stream
  );
  virtual ~WatchOutputMessageImpl();

  void set_uid(uint32_t uid) override;
  void set_status(watch::Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(mhconfig::ElementRef element) override;
  void set_element_bytes(const char* data, size_t len) override;

  bool send(bool finish = false) override;

protected:
  friend class Stream<grpc::ByteBuffer, grpc::ByteBuffer, WatchOutputMessageImpl>;

  grpc::ByteBuffer response_;

private:
  std::shared_ptr<WatchStreamImpl> stream_;
  mhconfig::proto::WatchResponse proto_response_;
  std::stringstream elements_data_;

  grpc::Slice slice_;
};

class WatchInputMessageImpl : public WatchInputMessage
{
public:
  WatchInputMessageImpl(
    std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
    std::shared_ptr<WatchStreamImpl> stream
  );
  virtual ~WatchInputMessageImpl();

  const uint32_t uid() const override;
  const bool remove() const override;
  const std::string& root_path() const override;
  const std::vector<std::string>& overrides() const override;
  const uint32_t version() const override;
  const std::string& document() const override;

  std::shared_ptr<WatchOutputMessage> make_output_message() override;

private:
  std::unique_ptr<mhconfig::proto::WatchRequest> request_;
  std::shared_ptr<WatchStreamImpl> stream_;

  std::vector<std::string> overrides_;
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
  const uint32_t version() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& key() const override;

  void set_status(::mhconfig::api::request::get_request::Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_element(mhconfig::ElementRef element) override;
  void set_element_bytes(const char* data, size_t len) override;

  bool commit() override;

private:
  std::shared_ptr<WatchInputMessage> input_message_;
  std::shared_ptr<WatchOutputMessage> output_message_;

  std::vector<std::string> key_;
};

class WatchStreamImpl
  : public Stream<grpc::ByteBuffer, grpc::ByteBuffer, WatchOutputMessageImpl>,
    public std::enable_shared_from_this<WatchStreamImpl>
{
public:
  WatchStreamImpl(
    CustomService* service,
    grpc::ServerCompletionQueue* cq,
    Metrics& metrics,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~WatchStreamImpl();

  const std::string name() const override;

  std::shared_ptr<Session> clone() override;
  void subscribe() override;

protected:
  friend class WatchOutputMessageImpl;

  void request(std::unique_ptr<grpc::ByteBuffer>&& raw_req) override;

private:
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;
  std::unordered_map<uint32_t, std::shared_ptr<WatchInputMessage>> watcher_by_id_;
};


} /* stream */
} /* api */
} /* mhconfig */

#endif