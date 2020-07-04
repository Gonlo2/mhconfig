#ifndef MHCONFIG__WORKER__UNREGISTER_WATCHERS_COMMAND_H
#define MHCONFIG__WORKER__UNREGISTER_WATCHERS_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"

namespace mhconfig
{
namespace worker
{

class UnregisterWatchersCommand : public WorkerCommand
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

} /* worker */
} /* mhconfig */

#endif
