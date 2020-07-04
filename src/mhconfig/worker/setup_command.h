#ifndef MHCONFIG__WORKER__SETUP_COMMAND_H
#define MHCONFIG__WORKER__SETUP_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/scheduler/add_namespace_command.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{

class SetupCommand : public WorkerCommand
{
public:
  explicit SetupCommand(const std::string& path);
  virtual ~SetupCommand();

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::string path_;
};

} /* worker */
} /* mhconfig */

#endif
