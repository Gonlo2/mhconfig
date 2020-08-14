#include "mhconfig/worker/gc_merged_configs_command.h"

namespace mhconfig
{
namespace worker
{

GCMergedConfigsCommand::GCMergedConfigsCommand(
  uint8_t generation,
  uint64_t timelimit_s
)
  : generation_(generation),
  timelimit_s_(timelimit_s)
{
}

bool GCMergedConfigsCommand::force_take_metric() const {
  return true;
}

std::string GCMergedConfigsCommand::name() const {
  return fmt::format("GC_MERGED_CONFIGS_GEN_{}", generation_);
}

bool GCMergedConfigsCommand::execute(
  context_t* ctx
) {
  ctx->mutex.ReaderLock();
  for (auto& it : ctx->cn_by_root_path) {
    gc_cn_merged_configs(it.second.get(), generation_, timelimit_s_);
  }
  ctx->mutex.ReaderUnlock();

  return true;
}

} /* worker */
} /* mhconfig */
