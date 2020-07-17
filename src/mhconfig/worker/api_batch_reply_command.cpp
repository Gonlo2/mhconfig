#include "mhconfig/worker/api_batch_reply_command.h"

namespace mhconfig
{
namespace worker
{

ApiBatchReplyCommand::ApiBatchReplyCommand(
  std::vector<std::shared_ptr<api::Commitable>>&& commitables
)
  : WorkerCommand(),
  commitables_(std::move(commitables))
{
}

ApiBatchReplyCommand::~ApiBatchReplyCommand() {
}

std::string ApiBatchReplyCommand::name() const {
  return "API_BATCH_REPLY";
}

bool ApiBatchReplyCommand::execute(
  context_t& context
) {
  for (auto& x : commitables_) {
    x->commit();
  }
  return true;
}

} /* worker */
} /* mhconfig */
