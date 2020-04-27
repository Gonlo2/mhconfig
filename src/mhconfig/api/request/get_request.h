#ifndef MHCONFIG__API__REQUEST__GET_REQUEST_H
#define MHCONFIG__API__REQUEST__GET_REQUEST_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/api_get_command.h"
//#include "mhconfig/api/config/merged_config.h"
//#include "mhconfig/worker/common.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace get_request
{

using jmutils::container::Queue;


class GetRequest : public Request
{
public:
  GetRequest(
      mhconfig::proto::MHConfig::AsyncService* service,
      grpc::ServerCompletionQueue* cq_,
      Metrics& metrics,
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~GetRequest();

  const std::string name() const override;
  const uint32_t id() const override;

  Request* clone() override;
  void subscribe() override;

  const std::string& root_path() const;
  const uint32_t version() const;
  const std::vector<std::string>& overrides() const;
  const std::vector<std::string>& key() const;

  void set_namespace_id(uint64_t namespace_id);
  void set_version(uint32_t version);
  void set_element(mhconfig::ElementRef element);

  mhconfig::proto::GetResponse& response();

protected:
  grpc::ServerAsyncResponseWriter<mhconfig::proto::GetResponse> responder_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  mhconfig::proto::GetRequest request_;
  mhconfig::proto::GetResponse response_;

  std::vector<std::string> overrides_;
  std::vector<std::string> key_;

  mhconfig::ElementRef element_{nullptr};

  void request() override;
  void finish() override;
};

} /* get_request */
} /* request */
} /* api */
} /* mhconfig */

#endif
