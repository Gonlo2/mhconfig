#include "mhconfig/worker/api_get_reply_command.h"

namespace mhconfig
{
namespace worker
{

ApiGetReplyCommand::ApiGetReplyCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
  const std::shared_ptr<merged_config_t>& merged_config,
  MergedConfigStatus status
)
  : request_(std::move(request)),
  merged_config_(merged_config),
  status_(status)
{
}

ApiGetReplyCommand::ApiGetReplyCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
  std::shared_ptr<merged_config_t>&& merged_config,
  MergedConfigStatus status
)
  : request_(std::move(request)),
  merged_config_(std::move(merged_config)),
  status_(status)
{
}

ApiGetReplyCommand::~ApiGetReplyCommand() {
}

std::string ApiGetReplyCommand::name() const {
  return "API_GET_REPLY";
}

bool ApiGetReplyCommand::execute(
  context_t& context
) {
  switch (status_) {
    case MergedConfigStatus::UNDEFINED:  // Fallback
    case MergedConfigStatus::BUILDING:
      assert(false);
    case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED: {
      proto::GetResponse get_response;
      auto checksum = merged_config_->value.make_checksum();
      get_response.set_checksum(checksum.data(), checksum.size());
      api::config::fill_elements(
        merged_config_->value,
        &get_response,
        get_response.add_elements()
      );

      if (get_response.SerializeToString(&merged_config_->preprocesed_value)) {
        context.async_metrics_service->add(
          metrics::MetricsService::MetricId::OPTIMIZED_MERGED_CONFIG_USED_BYTES,
          {},
          merged_config_->preprocesed_value.size()
        );
        request_->set_preprocessed_payload(
          merged_config_->preprocesed_value.c_str(),
          merged_config_->preprocesed_value.size()
        );

        context.scheduler_queue->push(
          std::make_unique<::mhconfig::scheduler::SetOptimizedConfigCommand>(
            std::move(merged_config_)
          )
        );
      } else {
        spdlog::warn("Can't optimize the config of the document");
        request_->set_checksum(checksum.data(), checksum.size());
        request_->set_element(merged_config_->value);
      }
      break;
    }
    case MergedConfigStatus::OK_CONFIG_OPTIMIZING: {
      // Maybe it's better wait to the optimized value to avoid
      // make the checksum again
      auto checksum = merged_config_->value.make_checksum();
      request_->set_checksum(checksum.data(), checksum.size());
      request_->set_element(merged_config_->value);
      break;
    }
    case MergedConfigStatus::OK_CONFIG_OPTIMIZED:
      request_->set_preprocessed_payload(
        merged_config_->preprocesed_value.c_str(),
        merged_config_->preprocesed_value.size()
      );
      break;
  }
  request_->commit();
  return true;
}

} /* worker */
} /* mhconfig */
