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
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
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

NamespaceExecutionResult ApiUpdateCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::UpdateCommand>(
      config_namespace.id,
      config_namespace.pool,
      update_request_
    )
  );

  return NamespaceExecutionResult::OK;
}

bool ApiUpdateCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  update_request_->set_status(::mhconfig::api::request::UpdateRequest::Status::ERROR);
  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(update_request_)
    )
  );

  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
