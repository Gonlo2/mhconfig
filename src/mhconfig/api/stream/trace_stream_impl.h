#ifndef MHCONFIG__API__STREAM__TRACE_STREAM_IMPL_H
#define MHCONFIG__API__STREAM__TRACE_STREAM_IMPL_H

#include "mhconfig/api/stream/stream.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/config/common.h"
#include "mhconfig/command.h"
#include "mhconfig/scheduler/api_trace_command.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

class TraceStreamImpl;

class TraceOutputMessageImpl : public TraceOutputMessage, public std::enable_shared_from_this<TraceOutputMessageImpl>
{
public:
  TraceOutputMessageImpl(
    std::weak_ptr<TraceStreamImpl>&& stream
  );
  virtual ~TraceOutputMessageImpl();

  void set_status(Status status) override;
  void set_namespace_id(uint64_t namespace_id) override;
  void set_version(uint32_t version) override;
  void set_overrides(const std::vector<std::string>& overrides) override;
  void set_flavors(const std::vector<std::string>& flavors) override;
  void set_document(const std::string& document) override;
  void set_template(const std::string& template_) override;
  void set_peer(const std::string& peer) override;

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
    public std::enable_shared_from_this<TraceStreamImpl>
{
public:
  TraceStreamImpl();
  virtual ~TraceStreamImpl();

  const std::string& root_path() const override;
  const std::vector<std::string>& overrides() const override;
  const std::vector<std::string>& flavors() const override;
  const std::string& document() const override;
  const std::string& template_() const override;

  std::shared_ptr<TraceOutputMessage> make_output_message() override;

  const std::string name() const override;

  void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

protected:
  friend class TraceOutputMessageImpl;

  void on_create(
    SchedulerQueue::Sender* scheduler_sender
  ) override;

  void on_read(
    SchedulerQueue::Sender* scheduler_sender
  ) override;

private:
  google::protobuf::Arena arena_;
  mhconfig::proto::TraceRequest *request_;

  std::vector<std::string> overrides_;
  std::vector<std::string> flavors_;
};

} /* stream */
} /* api */
} /* mhconfig */

#endif
