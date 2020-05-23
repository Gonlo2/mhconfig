#ifndef MHCONFIG__WORKER__COMMAND__UNREGISTER_WATCHERS_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__UNREGISTER_WATCHERS_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/worker/command/command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

class UnregisterWatchersCommand : public Command
{
public:
  UnregisterWatchersCommand(
    std::unordered_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>>&& watchers_to_remove
  );
  virtual ~UnregisterWatchersCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::unordered_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers_to_remove_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
