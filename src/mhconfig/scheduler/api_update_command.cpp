#include "mhconfig/scheduler/api_update_command.h"

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/update_command.h"
#include "mhconfig/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{

ApiUpdateCommand::ApiUpdateCommand(
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
) : SchedulerCommand(),
    update_request_(update_request)
{
}

ApiUpdateCommand::~ApiUpdateCommand() {
}

std::string ApiUpdateCommand::name() const {
  return "API_UPDATE";
}

SchedulerCommand::CommandType ApiUpdateCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiUpdateCommand::namespace_path() const {
  return update_request_->root_path();
}

SchedulerCommand::CommandResult ApiUpdateCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<worker::UpdateCommand>(
      config_namespace.id,
      config_namespace.pool,
      update_request_
    )
  );

  return CommandResult::OK;
}

bool ApiUpdateCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  update_request_->set_status(::mhconfig::api::request::UpdateRequest::Status::ERROR);
  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(update_request_)
    )
  );

  return true;
}

} /* scheduler */
} /* mhconfig */
