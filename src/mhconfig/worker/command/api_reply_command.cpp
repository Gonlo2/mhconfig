#include "mhconfig/worker/command/api_reply_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ApiReplyCommand::ApiReplyCommand(
  ::mhconfig::api::request::Request* request
)
  : Command(),
  request_(request)
{
}

ApiReplyCommand::~ApiReplyCommand() {
}

std::string ApiReplyCommand::name() const {
  return "API_REPLY";
}

bool ApiReplyCommand::execute(
  Queue<scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) {
  request_->reply();
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
