#ifndef MHCONFIG__SCHEDULER__API_TRACE_COMMAND_H
#define MHCONFIG__SCHEDULER__API_TRACE_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/stream/trace_stream_impl.h"
#include "mhconfig/scheduler/common.h"
#include "mhconfig/worker/api_reply_command.h"
#include "mhconfig/worker/api_batch_reply_command.h"
#include "mhconfig/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{

class ApiTraceCommand : public SchedulerCommand
{
public:
  ApiTraceCommand(
    std::shared_ptr<api::stream::TraceInputMessage>&& trace_stream
  );
  virtual ~ApiTraceCommand();

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
  std::shared_ptr<api::stream::TraceInputMessage> trace_stream_;

  void send_api_response(
    WorkerQueue& worker_queue
  );

};

} /* scheduler */
} /* mhconfig */

#endif
