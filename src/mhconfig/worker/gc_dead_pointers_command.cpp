#include "mhconfig/worker/gc_dead_pointers_command.h"

namespace mhconfig
{
namespace worker
{

bool GCDeadPointersCommand::force_take_metric() const {
  return true;
}

std::string GCDeadPointersCommand::name() const {
  return "GC_DEAD_POINTERS";
}

bool GCDeadPointersCommand::execute(
  context_t* ctx
) {
  ctx->mutex.ReaderLock();
  for (auto& it : ctx->cn_by_root_path) {
    gc_cn_dead_pointers(it.second.get());
  }
  ctx->mutex.ReaderUnlock();

  return true;
}

} /* worker */
} /* mhconfig */
