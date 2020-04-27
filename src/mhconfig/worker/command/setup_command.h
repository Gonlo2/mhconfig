#ifndef MHCONFIG__WORKER__COMMAND__SETUP_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__SETUP_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/worker/command/command.h"
#include "mhconfig/scheduler/command/add_namespace_command.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

class SetupCommand : public Command
{
public:
  SetupCommand(const std::string& path);
  virtual ~SetupCommand();

  bool execute(
    Queue<scheduler::command::CommandRef>& scheduler_queue
  ) override;

private:
  std::string path_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
