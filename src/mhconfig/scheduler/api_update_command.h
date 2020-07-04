#ifndef MHCONFIG__SCHEDULER__API_UPDATE_COMMAND_H
#define MHCONFIG__SCHEDULER__API_UPDATE_COMMAND_H

#include <memory>

#include "mhconfig/api/request/update_request.h"
#include "mhconfig/command.h"

namespace mhconfig
{
namespace scheduler
{

class ApiUpdateCommand : public SchedulerCommand
{
public:
  ApiUpdateCommand(
    std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
  );
  virtual ~ApiUpdateCommand();

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
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request_;
};

} /* scheduler */
} /* mhconfig */

#endif
