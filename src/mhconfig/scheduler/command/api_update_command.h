#ifndef MHCONFIG__SCHEDULER__COMMAND__API_UPDATE_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_UPDATE_COMMAND_H

#include <memory>

#include "mhconfig/api/request/update_request.h"
#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

class ApiUpdateCommand : public Command
{
public:
  ApiUpdateCommand(
    std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
  );
  virtual ~ApiUpdateCommand();

  std::string name() const override;

  CommandType command_type() const override;

  const std::string& namespace_path() const override;

  NamespaceExecutionResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
