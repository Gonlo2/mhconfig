#include "mhconfig/worker/command/optimize_merged_config_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

OptimizeMergedConfigCommand::OptimizeMergedConfigCommand(
  std::shared_ptr<merged_config_t> merged_config
)
  : Command(),
  merged_config_(merged_config)
{
}

OptimizeMergedConfigCommand::~OptimizeMergedConfigCommand() {
}

std::string OptimizeMergedConfigCommand::name() const {
  return "OPTIMIZE_MERGED_CONFIG";
}

bool OptimizeMergedConfigCommand::force_take_metric() const {
  return true;
}

bool OptimizeMergedConfigCommand::execute(
  context_t& context
) {
  ::mhconfig::proto::GetResponse get_response;
  std::string data;

  ::mhconfig::api::config::fill_elements(
    merged_config_->value.get(),
    &get_response,
    get_response.add_elements()
  );

  if (get_response.SerializeToString(&data)) {
    context.metrics_service.observe(
      metrics::MetricsService::ObservableId::OPTIMIZED_MERGED_CONFIG_USED_BYTES,
      {},
      data.size()
    );

    // This works only because the the api merged config object is modified only in the
    // scheduler thread and that modification trigger this command, so at this point
    // this thread is the only place where this variable is modified.
    // If some other MergedConfig strategy is made THIS SHOULD BE CHANGED TO SWAP IT IN
    // THE SCHEDULER THREAD
    //TODO
    //merged_config_->api_merged_config = std::make_shared<mhconfig::api::config::OptimizedMergedConfig>(std::move(data));
  } else {
    spdlog::warn("Can't optimize the config of the document");
  }
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
