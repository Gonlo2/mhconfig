#include "mhconfig/worker/gc_config_namespaces_command.h"

namespace mhconfig
{
namespace worker
{

GCConfigNamespacesCommand::GCConfigNamespacesCommand(
  uint64_t timelimit_s
)
  : timelimit_s_(timelimit_s)
{
}

bool GCConfigNamespacesCommand::force_take_metric() const {
  return true;
}

std::string GCConfigNamespacesCommand::name() const {
  return "GC_CONFIG_NAMESPACES";
}

bool GCConfigNamespacesCommand::execute(
  context_t* ctx
) {
  gc_cn(ctx, timelimit_s_);

  return true;
}

} /* worker */
} /* mhconfig */
