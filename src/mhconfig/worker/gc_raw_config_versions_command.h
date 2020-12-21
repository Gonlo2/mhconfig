#ifndef MHCONFIG__WORKER__GC_RAW_CONFIG_VERSIONS_COMMAND_H
#define MHCONFIG__WORKER__GC_RAW_CONFIG_VERSIONS_COMMAND_H

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

class GCRawConfigVersionsCommand final : public WorkerCommand
{
public:
  GCRawConfigVersionsCommand(
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
