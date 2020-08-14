#include "mhconfig/worker/optimize_command.h"

namespace mhconfig
{
namespace worker
{

OptimizeCommand::OptimizeCommand(
  std::shared_ptr<merged_config_t>&& merged_config
)
  : merged_config_(std::move(merged_config))
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
  proto::GetResponse get_response;
  auto checksum = merged_config_->value.make_checksum();
  get_response.set_checksum(checksum.data(), checksum.size());
  api::config::fill_elements(
    merged_config_->value,
    &get_response,
    get_response.add_elements()
  );

  std::string buffer;
  bool ok = get_response.SerializeToString(&buffer);
  if (ok) {
    context->metrics.add(
      Metrics::Id::OPTIMIZED_MERGED_CONFIG_USED_BYTES,
      {},
      buffer.size()
    );
  }

  std::vector<std::shared_ptr<api::request::GetRequest>> waiting;

  merged_config_->mutex.Lock();
  if (ok) {
    merged_config_->status = MergedConfigStatus::OK_CONFIG_OPTIMIZED;
    std::swap(merged_config_->preprocesed_value, buffer);
  } else {
    merged_config_->status = MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED;
  }
  std::swap(merged_config_->waiting, waiting);
  merged_config_->mutex.Unlock();

  if (ok) {
    for (size_t i = 0, l = waiting.size(); i < l; ++i) {
      waiting[i]->set_preprocessed_payload(
        merged_config_->preprocesed_value.data(),
        merged_config_->preprocesed_value.size()
      );
      waiting[i]->commit();
    }
  } else {
    spdlog::warn("Can't optimize the config of the document");
    for (size_t i = 0, l = waiting.size(); i < l; ++i) {
      waiting[i]->set_checksum(checksum.data(), checksum.size());
      waiting[i]->set_element(merged_config_->value);
      waiting[i]->commit();
    }
  }

  return true;
}

} /* worker */
} /* mhconfig */
