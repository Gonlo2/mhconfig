#ifndef MHCONFIG__WORKER__SETUP_COMMAND_H
#define MHCONFIG__WORKER__SETUP_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/builder.h"
#include "mhconfig/provider.h"
#include "mhconfig/worker/update_command.h"
#include "mhconfig/api/stream/watch_stream_impl.h"

namespace mhconfig
{
namespace worker
{

class SetupCommand final : public WorkerCommand
{
public:
  explicit SetupCommand(std::shared_ptr<config_namespace_t>&& cn);

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t* ctx
  ) override;

private:
  std::shared_ptr<config_namespace_t> cn_;
};

} /* worker */
} /* mhconfig */

#endif
