#ifndef MHCONFIG__WORKER__COMMAND__API_GET_REPLY_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__API_GET_REPLY_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/worker/command/command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/merged_config.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

using namespace mhconfig::ds::config_namespace;

class ApiGetReplyCommand : public Command
{
public:

  ApiGetReplyCommand(
    std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  );
  virtual ~ApiGetReplyCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::shared_ptr<::mhconfig::api::request::GetRequest> request_;
  std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
