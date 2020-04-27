#ifndef MHCONFIG__API__REQUEST__UPDATE_REQUEST_H
#define MHCONFIG__API__REQUEST__UPDATE_REQUEST_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/scheduler/command/command.h"
//#include "mhconfig/worker/common.h"

namespace mhconfig
{
namespace api
{
namespace request
{
namespace update_request
{

using jmutils::container::Queue;
using namespace mhconfig::scheduler::command;

enum Status {
  OK,
  ERROR
};

class UpdateRequest : public Request
{
public:
  UpdateRequest(
      mhconfig::proto::MHConfig::AsyncService* service,
      grpc::ServerCompletionQueue* cq_,
      Metrics& metrics,
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue
  );
  virtual ~UpdateRequest();

  const std::string name() const override;
  const uint32_t id() const override;

  Request* clone() override;
  void subscribe() override;

  const std::string& root_path() const;
  const std::vector<std::string>& relative_paths() const;

  void set_namespace_id(uint64_t namespace_id);
  void set_status(update_request::Status status);
  void set_version(uint32_t version);

protected:
  grpc::ServerAsyncResponseWriter<mhconfig::proto::UpdateResponse> responder_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  mhconfig::proto::UpdateRequest request_;
  mhconfig::proto::UpdateResponse response_;

  std::vector<std::string> relative_paths_;

  void request() override;
  void finish() override;
};

} /* update_request */
} /* request */
} /* api */
} /* mhconfig */

#endif
