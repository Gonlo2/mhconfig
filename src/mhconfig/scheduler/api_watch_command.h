#ifndef MHCONFIG__SCHEDULER__API_WATCH_COMMAND_H
#define MHCONFIG__SCHEDULER__API_WATCH_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/worker/build_command.h"
#include "mhconfig/worker/api_reply_command.h"
#include "mhconfig/command.h"
#include "mhconfig/scheduler/api_get_command.h"
#include "mhconfig/config_namespace.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{

using namespace ::mhconfig::builder;

class ApiWatchCommand : public SchedulerCommand
{
public:
  ApiWatchCommand(
    std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message
  );
  virtual ~ApiWatchCommand();

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
  std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message_;
};

} /* scheduler */
} /* mhconfig */

#endif
