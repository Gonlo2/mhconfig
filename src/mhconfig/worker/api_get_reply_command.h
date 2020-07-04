#ifndef MHCONFIG__WORKER__API_GET_REPLY_COMMAND_H
#define MHCONFIG__WORKER__API_GET_REPLY_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/scheduler/set_optimized_config_command.h"
#include "mhconfig/command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/common.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace worker
{

class ApiGetReplyCommand : public WorkerCommand
{
public:

  ApiGetReplyCommand(
    std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
    const std::shared_ptr<merged_config_t>& merged_config,
    MergedConfigStatus status
  );

  ApiGetReplyCommand(
    std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
    std::shared_ptr<merged_config_t>&& merged_config,
    MergedConfigStatus status
  );

  virtual ~ApiGetReplyCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::shared_ptr<::mhconfig::api::request::GetRequest> request_;
  std::shared_ptr<merged_config_t> merged_config_;
  MergedConfigStatus status_;
};

} /* worker */
} /* mhconfig */

#endif
