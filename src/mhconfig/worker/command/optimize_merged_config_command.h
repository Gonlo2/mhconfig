#ifndef MHCONFIG__WORKER__COMMAND__OPTIMIZE_MERGED_CONFIG_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__OPTIMIZE_MERGED_CONFIG_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/api/config/common.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/builder.h"

#include "string_pool/pool.h"

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

  bool force_take_metric() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::shared_ptr<merged_config_t> merged_config_;
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
