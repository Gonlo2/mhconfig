#ifndef MHCONFIG__SCHEDULER__ON_WATCHERS_REMOVED_COMMAND_H
#define MHCONFIG__SCHEDULER__ON_WATCHERS_REMOVED_COMMAND_H

#include <memory>

#include "mhconfig/worker/api_batch_reply_command.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/scheduler/common.h"
#include "mhconfig/command.h"

namespace mhconfig
{
namespace scheduler
{

class OnWatchersRemovedCommand : public SchedulerCommand
{
public:
  OnWatchersRemovedCommand(
    std::shared_ptr<api::stream::WatchInputMessage>&& watcher
  );
  OnWatchersRemovedCommand(
    const std::string& root_path,
    std::vector<std::shared_ptr<api::stream::WatchInputMessage>>&& watchers
  );
  virtual ~OnWatchersRemovedCommand();

  std::string name() const override;

  CommandType type() const override;
  const std::string& namespace_path() const override;

  CommandResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  std::string root_path_;
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watchers_;
};

} /* scheduler */
} /* mhconfig */

#endif
