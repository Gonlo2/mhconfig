#ifndef MHCONFIG__WORKER__COMMAND__API_REPLY_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__API_REPLY_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/worker/command/command.h"
#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

using namespace mhconfig::ds::config_namespace;

class ApiReplyCommand : public Command
{
public:
  ApiReplyCommand(
    std::shared_ptr<::mhconfig::api::Commitable> commitable
  );
  virtual ~ApiReplyCommand();

  std::string name() const override;

  bool execute(
    Queue<scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  ) override;

private:
  std::shared_ptr<::mhconfig::api::Commitable> commitable_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
