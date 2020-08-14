#ifndef MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H
#define MHCONFIG__API__REQUEST__UPDATE_REQUEST_IMPL_H

#include "jmutils/container/queue.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/worker/setup_command.h"
#include "mhconfig/worker/update_command.h"
#include "mhconfig/command.h"
#include "mhconfig/validator.h"

namespace mhconfig
{
namespace api
{
namespace request
{

class UpdateRequestImpl : public Request, public UpdateRequest, public std::enable_shared_from_this<UpdateRequestImpl>
{
public:
  UpdateRequestImpl();
  virtual ~UpdateRequestImpl();

  const std::string name() const override;

  void clone_and_subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;
  void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) override;

  const std::string& root_path() const override;
  const std::vector<std::string>& relative_paths() const override;
  bool reload() const override;

  void set_namespace_id(uint64_t namespace_id) override;
  void set_status(Status status) override;
  void set_version(uint32_t version) override;

  bool commit() override;

  bool finish(const grpc::Status& status = grpc::Status::OK) override;

protected:
  google::protobuf::Arena arena_;
  grpc::ServerAsyncResponseWriter<mhconfig::proto::UpdateResponse> responder_;

  mhconfig::proto::UpdateRequest* request_;
  mhconfig::proto::UpdateResponse* response_;

  std::vector<std::string> relative_paths_;

  void request(
    context_t* ctx
  ) override;

private:
  bool validate_request();
};

} /* request */
} /* api */
} /* mhconfig */

#endif
