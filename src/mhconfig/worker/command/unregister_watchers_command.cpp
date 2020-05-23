#include "mhconfig/worker/command/unregister_watchers_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

UnregisterWatchersCommand::UnregisterWatchersCommand(
  std::unordered_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>>&& watchers_to_remove
)
  : watchers_to_remove_(std::move(watchers_to_remove))
{
}

UnregisterWatchersCommand::~UnregisterWatchersCommand() {
}

std::string UnregisterWatchersCommand::name() const {
  return "UNREGISTER_WATCHERS";
}

bool UnregisterWatchersCommand::execute(
  context_t& context
) {
  for (auto& watcher : watchers_to_remove_) {
    watcher->unregister();
  }
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
