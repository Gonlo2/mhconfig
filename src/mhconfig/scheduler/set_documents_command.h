#ifndef MHCONFIG__SCHEDULER__SET_DOCUMENTS_COMMAND_H
#define MHCONFIG__SCHEDULER__SET_DOCUMENTS_COMMAND_H

#include <memory>

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/command.h"
#include "mhconfig/worker/api_get_reply_command.h"
#include "mhconfig/worker/api_reply_command.h"
#include "mhconfig/worker/build_command.h"
#include "mhconfig/builder.h"
#include "jmutils/time.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace scheduler
{

class SetDocumentsCommand : public SchedulerCommand
{
public:
  SetDocumentsCommand(
    uint64_t namespace_id,
    std::shared_ptr<build::wait_built_t>&& wait_build
  );

  virtual ~SetDocumentsCommand();

  std::string name() const override;

  CommandType type() const override;
  uint64_t namespace_id() const override;

  CommandResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<build::wait_built_t> wait_build_;
};

} /* scheduler */
} /* mhconfig */

#endif
