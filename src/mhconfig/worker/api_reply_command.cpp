#include "mhconfig/worker/api_reply_command.h"

namespace mhconfig
{
namespace worker
{

ApiReplyCommand::ApiReplyCommand(
  std::shared_ptr<::mhconfig::api::Commitable>&& commitable
)
  : WorkerCommand(),
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

} /* worker */
} /* mhconfig */
