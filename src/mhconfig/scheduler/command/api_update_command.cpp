#include "mhconfig/scheduler/command/api_update_command.h"

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/command/update_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ApiUpdateCommand::ApiUpdateCommand(
  ::mhconfig::api::request::UpdateRequest* update_request
) : Command(),
    update_request_(update_request)
{
}

ApiUpdateCommand::~ApiUpdateCommand() {
}

std::string ApiUpdateCommand::name() const {
  return "API_UPDATE";
}

CommandType ApiUpdateCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiUpdateCommand::namespace_path() const {
  return update_request_->root_path();
}

bool ApiUpdateCommand::execute_on_namespace(
  std::shared_ptr<config_namespace_t> config_namespace,
  Queue<worker::command::CommandRef>& worker_queue
) {
  auto update_command = std::make_shared<::mhconfig::worker::command::UpdateCommand>(
    config_namespace->id,
    config_namespace->pool,
    update_request_
  );
  worker_queue.push(update_command);
  return true;
}

bool ApiUpdateCommand::on_get_namespace_error(
  Queue<worker::command::CommandRef>& worker_queue
) {
  //TODO
  return false;
}

} /* command */
} /* scheduler */
} /* mhconfig */
