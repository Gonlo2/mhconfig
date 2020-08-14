#include "mhconfig/worker/gc_raw_config_versions_command.h"

namespace mhconfig
{
namespace worker
{

GCRawConfigVersionsCommand::GCRawConfigVersionsCommand(
  uint64_t timelimit_s
)
  : timelimit_s_(timelimit_s)
{
}

bool GCRawConfigVersionsCommand::force_take_metric() const {
  return true;
}

std::string GCRawConfigVersionsCommand::name() const {
  return "GC_RAW_CONFIG_VERSIONS";
}

bool GCRawConfigVersionsCommand::execute(
  context_t* ctx
) {
  ctx->mutex.ReaderLock();
  for (auto& it : ctx->cn_by_root_path) {
    gc_cn_raw_config_versions(it.second.get(), timelimit_s_);
  }
  ctx->mutex.ReaderUnlock();

  return true;
}

} /* worker */
} /* mhconfig */
