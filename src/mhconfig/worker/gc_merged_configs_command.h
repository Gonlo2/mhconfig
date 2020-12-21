#ifndef MHCONFIG__WORKER__GC_MERGED_CONFIGS_COMMAND_H
#define MHCONFIG__WORKER__GC_MERGED_CONFIGS_COMMAND_H

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

class GCMergedConfigsCommand final : public WorkerCommand
{
public:
  GCMergedConfigsCommand(
    uint8_t generation,
    uint64_t timelimit_s
  );

  bool force_take_metric() const override;

  std::string name() const override;

  bool execute(
    context_t* ctx
  ) override;

private:
  uint8_t generation_;
  uint64_t timelimit_s_;
};

} /* worker */
} /* mhconfig */

#endif
