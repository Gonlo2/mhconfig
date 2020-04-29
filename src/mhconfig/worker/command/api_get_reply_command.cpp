#include "mhconfig/worker/command/api_get_reply_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ApiGetReplyCommand::ApiGetReplyCommand(
  ::mhconfig::api::request::GetRequest* request,
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
  Queue<scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) {
  request_->reply();
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
