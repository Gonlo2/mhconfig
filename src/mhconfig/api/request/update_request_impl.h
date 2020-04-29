#ifndef MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/api_update_command.h"

namespace mhconfig
{
namespace api
{
namespace request
{

using jmutils::container::Queue;


class UpdateRequestImpl : public UpdateRequest
{
public:
  UpdateRequestImpl(
      CustomService* service,
      grpc::ServerCompletionQueue* cq_,
      Metrics& metrics,
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~UpdateRequestImpl();

  const std::string name() const override;

  Request* clone() override;
  void subscribe() override;

  const std::string& root_path() const override;
  const std::vector<std::string>& relative_paths() const override;

  void set_namespace_id(uint64_t namespace_id) override;
  void set_status(update_request::Status status) override;
  void set_version(uint32_t version) override;

protected:
  grpc::ServerAsyncResponseWriter<mhconfig::proto::UpdateResponse> responder_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  mhconfig::proto::UpdateRequest request_;
  mhconfig::proto::UpdateResponse response_;

  std::vector<std::string> relative_paths_;

  void request() override;
  void finish() override;
};

} /* request */
} /* api */
} /* mhconfig */

#endif
