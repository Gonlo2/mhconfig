#ifndef MHCONFIG__SCHEDULER__COMMAND__API_WATCH_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_WATCH_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/worker/command/api_reply_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/scheduler/command/api_get_command.h"
#include "mhconfig/ds/config_namespace.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace ::mhconfig::builder;
using namespace ::mhconfig::ds::config_namespace;

class ApiWatchCommand : public Command
{
public:
  ApiWatchCommand(
    std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message
  );
  virtual ~ApiWatchCommand();

  std::string name() const override;

  CommandType command_type() const override;
  const std::string& namespace_path() const override;

  NamespaceExecutionResult execute_on_namespace(
    config_namespace_t& config_namespace,
    Queue<CommandRef>& scheduler_queue,
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

  bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

private:
  std::shared_ptr<::mhconfig::api::stream::WatchInputMessage> message_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
