#include "mhconfig/worker/command/unregister_watchers_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

UnregisterWatchersCommand::UnregisterWatchersCommand(
  std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>>&& watchers
)
  : watchers_(std::move(watchers))
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
  for (auto& weak_ptr : watchers_) {
    if (auto watcher = weak_ptr.lock()) {
      watcher->unregister();

      auto output_message = watcher->make_output_message();
      output_message->set_status(::mhconfig::api::stream::watch::Status::REMOVED);
      output_message->send();
    }
  }
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
