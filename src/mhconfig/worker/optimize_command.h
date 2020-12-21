#ifndef MHCONFIG__OPTIMIZE_COMMAND_H
#define MHCONFIG__OPTIMIZE_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/builder.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"

namespace mhconfig
{
namespace worker
{

class OptimizeCommand : public WorkerCommand
{
public:
  OptimizeCommand(
    std::shared_ptr<config_namespace_t>&& cn,
    std::shared_ptr<merged_config_t>&& merged_config
  );
  virtual ~OptimizeCommand();

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t* context
  ) override;

private:
  std::shared_ptr<config_namespace_t> cn_;
  std::shared_ptr<merged_config_t> merged_config_;
};

} /* worker */
} /* mhconfig */

#endif
