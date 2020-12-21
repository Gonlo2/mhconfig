#ifndef MHCONFIG__WORKER__GC_CONFIG_NAMESPACES_COMMAND_H
#define MHCONFIG__WORKER__GC_CONFIG_NAMESPACES_COMMAND_H

#include <bits/stdint-uintn.h>
#include <memory>
#include <string>

#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/gc.h"

namespace mhconfig
{
namespace worker
{

class GCConfigNamespacesCommand final : public WorkerCommand
{
public:
  GCConfigNamespacesCommand(
    uint64_t timelimit_s
  );

  bool force_take_metric() const override;

  std::string name() const override;

  bool execute(
    context_t* ctx
  ) override;

private:
  uint64_t timelimit_s_;
};

} /* worker */
} /* mhconfig */

#endif
