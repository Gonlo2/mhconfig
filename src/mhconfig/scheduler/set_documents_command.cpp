#include "mhconfig/scheduler/set_documents_command.h"

namespace mhconfig
{
namespace scheduler
{

SetDocumentsCommand::SetDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<build::wait_built_t>&& wait_build
)
  : SchedulerCommand(),
  namespace_id_(namespace_id),
  wait_build_(std::move(wait_build))
{
}

SetDocumentsCommand::~SetDocumentsCommand() {
}

std::string SetDocumentsCommand::name() const {
  return "SET_DOCUMENTS";
}

SchedulerCommand::CommandType SetDocumentsCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_ID;
}

uint64_t SetDocumentsCommand::namespace_id() const {
  return namespace_id_;
}

SchedulerCommand::CommandResult SetDocumentsCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  spdlog::debug(
    "Processing the build response with id {}",
    (void*) wait_build_->request.get()
  );

  bool store_optimized_config = wait_build_->is_preprocesed_value_ok;

  for (size_t i = 0, l = wait_build_->elements_to_build.size(); i < l; ++i) {
    auto& build_element = wait_build_->elements_to_build[i];
    if (build_element.to_build) {
      spdlog::debug("Setting the document '{}'", build_element.name);

      auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
        config_namespace,
        build_element.overrides_key
      );

      if ((i == l-1) && store_optimized_config) {
        merged_config->status = MergedConfigStatus::OK_CONFIG_OPTIMIZED;
        merged_config->preprocesed_value = std::move(wait_build_->preprocesed_value);
      } else {
        merged_config->status = MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED;
      }
      merged_config->last_access_timestamp = jmutils::monotonic_now_sec();
      merged_config->value = build_element.config;

      auto wait_builts_search = config_namespace.wait_builts_by_key
        .find(build_element.overrides_key);

      if (wait_builts_search != config_namespace.wait_builts_by_key.end()) {
        for (size_t i = 0; i < wait_builts_search->second.size(); ) {
          auto wait_built = wait_builts_search->second[i].get();

          spdlog::debug(
            "Unchecking element built for the request with id {}",
            (void*) wait_built->request.get()
          );
          for (size_t i = wait_built->elements_to_build.size(); i--;) {
            if (wait_built->elements_to_build[i].name == build_element.name) {
              wait_built->elements_to_build[i].config = build_element.config;
              wait_built->elements_to_build[i].to_build = false;
              break;
            }
          }
          wait_built->num_pending_elements -= 1;
          if (wait_built->num_pending_elements == 0) {
            // If the requested config don't have any ref and it don't have a template
            // we could send it directly to the API
            if (wait_built->elements_to_build.size() == 1) {
              spdlog::debug(
                "Responding the get request with id {}",
                (void*) wait_built->request.get()
              );

              auto status = merged_config->status;
              if (status == MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED) {
                merged_config->status = MergedConfigStatus::OK_CONFIG_OPTIMIZING;
              }

              worker_queue.push(
                std::make_unique<worker::ApiGetReplyCommand>(
                  std::move(wait_built->request),
                  merged_config,
                  status
                )
              );
            } else {
              spdlog::debug(
                "Sending the get request with id {} to built",
                (void*) wait_built->request.get()
              );

              worker_queue.push(
                std::make_unique<worker::BuildCommand>(
                  config_namespace.id,
                  config_namespace.pool,
                  std::move(wait_builts_search->second[i])
                )
              );
            }

            jmutils::swap_delete(wait_builts_search->second, i);
          } else {
            ++i;
          }
        }
        if (wait_builts_search->second.empty()) {
          config_namespace.wait_builts_by_key.erase(wait_builts_search);
        }
      }
    }
  }

  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    wait_build_->overrides_key
  );

  auto status = merged_config->status;
  worker_queue.push(
    std::make_unique<worker::ApiGetReplyCommand>(
      std::move(wait_build_->request),
      std::move(merged_config),
      status
    )
  );

  return CommandResult::OK;
}

bool SetDocumentsCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  wait_build_->request->set_element(UNDEFINED_ELEMENT);

  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(wait_build_->request)
    )
  );

  return true;
}

} /* scheduler */
} /* mhconfig */
