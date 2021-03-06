#include "mhconfig/worker/optimize_command.h"

namespace mhconfig
{
namespace worker
{

OptimizeCommand::OptimizeCommand(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<merged_config_t>&& merged_config
)
  : cn_(std::move(cn)),
  merged_config_(std::move(merged_config))
{
}

OptimizeCommand::~OptimizeCommand() {
}

std::string OptimizeCommand::name() const {
  return "OPTIMIZE";
}

bool OptimizeCommand::force_take_metric() const {
  return true;
}

bool OptimizeCommand::execute(
  context_t* context
) {
  std::vector<std::shared_ptr<GetConfigTask>> waiting;

  auto checksum = merged_config_->value.make_checksum();

  merged_config_->mutex.Lock();
  merged_config_->checksum = std::move(checksum);
  bool ok = alloc_payload_locked(merged_config_.get());
  std::swap(merged_config_->waiting, waiting);
  merged_config_->mutex.Unlock();

  for (size_t i = 0, l = waiting.size(); i < l; ++i) {
    if (!ok) {
      waiting[i]->logger().error("Some error take place allocating the payload");
    }
    waiting[i]->on_complete(
      cn_,
      0, //FIXME
      merged_config_->value,
      merged_config_->checksum,
      merged_config_->payload
    );
  }

  return true;
}

} /* worker */
} /* mhconfig */
