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
  explicit UnregisterWatchersCommand(
    std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>>&& watchers
  );
  virtual ~UnregisterWatchersCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
