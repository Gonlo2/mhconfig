#ifndef MHCONFIG__WORKER__COMMAND__BUILD_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__BUILD_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/scheduler/command/set_documents_command.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

using namespace mhconfig::ds::config_namespace;

class BuildCommand : public Command
{
public:

  BuildCommand(
    uint64_t namespace_id,
    std::shared_ptr<string_pool::Pool> pool,
    std::shared_ptr<build::wait_built_t> wait_build
  );
  virtual ~BuildCommand();

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t& context
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<string_pool::Pool> pool_;
  std::shared_ptr<build::wait_built_t> wait_build_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
