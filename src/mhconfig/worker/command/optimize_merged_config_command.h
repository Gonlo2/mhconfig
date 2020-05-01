#ifndef MHCONFIG__WORKER__COMMAND__OPTIMIZE_MERGED_CONFIG_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__OPTIMIZE_MERGED_CONFIG_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/api/config/optimized_merged_config.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

using namespace ::mhconfig::ds::config_namespace;

class OptimizeMergedConfigCommand : public Command
{
public:
  OptimizeMergedConfigCommand(
    std::shared_ptr<merged_config_t> merged_config
  );
  virtual ~OptimizeMergedConfigCommand();

  std::string name() const override;

  bool execute(
    Queue<scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  ) override;

private:
  std::shared_ptr<merged_config_t> merged_config_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
