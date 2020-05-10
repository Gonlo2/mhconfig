#include "mhconfig/worker/command/api_get_reply_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ApiGetReplyCommand::ApiGetReplyCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest> request,
  std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
)
  : Command(),
  request_(request),
  api_merged_config_(api_merged_config)
{
}

ApiGetReplyCommand::~ApiGetReplyCommand() {
}

std::string ApiGetReplyCommand::name() const {
  return "API_GET_REPLY";
}

bool ApiGetReplyCommand::execute(
  context_t& context
) {
  api_merged_config_->add_elements(request_.get());
  request_->commit();
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
