#ifndef MHCONFIG__WORKER__GC_DEAD_POINTERS_COMMAND_H
#define MHCONFIG__WORKER__GC_DEAD_POINTERS_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/gc.h"

namespace mhconfig
{
namespace worker
{

class GCDeadPointersCommand final : public WorkerCommand
{
public:
  bool force_take_metric() const override;

  std::string name() const override;

  bool execute(
    context_t* ctx
  ) override;
};

} /* worker */
} /* mhconfig */

#endif
