#include "mhconfig/worker/command/api_get_reply_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

ApiGetReplyCommand::ApiGetReplyCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
  const std::shared_ptr<mhconfig::ds::config_namespace::merged_config_t>& merged_config
)
  : request_(std::move(request)),
  merged_config_(merged_config)
{
}

ApiGetReplyCommand::ApiGetReplyCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest>&& request,
  std::shared_ptr<mhconfig::ds::config_namespace::merged_config_t>&& merged_config
)
  : request_(std::move(request)),
  merged_config_(std::move(merged_config))
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
  switch (merged_config_->status) {
    case MergedConfigStatus::UNDEFINED:  // Fallback
    case MergedConfigStatus::BUILDING:
      assert(false);
    case MergedConfigStatus::OK_CONFIG_NORMAL:
      request_->set_element(merged_config_->value.get());
      break;
    case MergedConfigStatus::OK_TEMPLATE:
      request_->set_template_rendered(merged_config_->preprocesed_value);
      break;
  }
  request_->commit();
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
