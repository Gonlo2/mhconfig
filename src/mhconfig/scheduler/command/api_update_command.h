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
    ::mhconfig::api::request::UpdateRequest* update_request
  );
  virtual ~ApiUpdateCommand();

  std::string name() const override;

  CommandType command_type() const override;

  const std::string& namespace_path() const override;

  bool execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

  bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

private:
  ::mhconfig::api::request::UpdateRequest* update_request_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
