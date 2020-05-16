#include "mhconfig/worker/command/api_reply_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ApiReplyCommand::ApiReplyCommand(
  std::shared_ptr<::mhconfig::api::Commitable>&& commitable
)
  : Command(),
  commitable_(std::move(commitable))
{
}

ApiReplyCommand::~ApiReplyCommand() {
}

std::string ApiReplyCommand::name() const {
  return "API_REPLY";
}

bool ApiReplyCommand::execute(
  context_t& context
) {
  commitable_->commit();
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
