#include "mhconfig/config_namespace.h"

namespace mhconfig
{

WorkerCommand::~WorkerCommand() {
}

bool WorkerCommand::force_take_metric() const {
  return false;
}

GetConfigTask::Status alloc_payload_locked(
  merged_config_t* merged_config
) {
  merged_config->payload = nullptr;
  if (merged_config->payload_fun.alloc(merged_config->value, merged_config->payload)) {
    merged_config->status = MergedConfigStatus::OK_CONFIG_OPTIMIZED;
    return GetConfigTask::Status::OK;
  }
  spdlog::error("Some error take place allocating the payload");
  if (merged_config->payload != nullptr) {
    merged_config->payload_fun.dealloc(merged_config->payload);
    merged_config->payload = nullptr;
  }
  merged_config->status = MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED;
  return GetConfigTask::Status::ERROR;
}

} /* mhconfig */
