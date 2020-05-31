#include "mhconfig/scheduler/command/set_documents_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

SetDocumentsCommand::SetDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<build::wait_built_t>&& wait_build
)
  : Command(),
  namespace_id_(namespace_id),
  wait_build_(std::move(wait_build))
{
}

SetDocumentsCommand::~SetDocumentsCommand() {
}

std::string SetDocumentsCommand::name() const {
  return "SET_DOCUMENTS";
}

CommandType SetDocumentsCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_ID;
}

uint64_t SetDocumentsCommand::namespace_id() const {
  return namespace_id_;
}

NamespaceExecutionResult SetDocumentsCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  spdlog::debug(
    "Processing the build response with id {}",
    (void*) wait_build_->request.get()
  );

  for (auto& build_element : wait_build_->elements_to_build) {
    if (build_element.is_new_config) {
      spdlog::debug("Setting the document '{}'", build_element.name);

      auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
        config_namespace,
        build_element.overrides_key
      );
      merged_config->status = MergedConfigStatus::OK_CONFIG_NORMAL;
      merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();
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
          auto search = wait_built->pending_element_position_by_name
            .find(build_element.name);
          wait_built->elements_to_build[search->second].config = build_element.config;
          wait_built->pending_element_position_by_name.erase(search);

          if (wait_built->pending_element_position_by_name.empty()) {
            // If the requested config don't have any ref and it don't have a template
            // we could send it directly to the API
            if ((wait_built->elements_to_build.size() == 1) && (wait_built->template_ == nullptr)) {
              spdlog::debug(
                "Responding the get request with id {}",
                (void*) wait_built->request.get()
              );

              worker_queue.push(
                std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
                  std::move(wait_built->request),
                  merged_config
                )
              );
            } else {
              spdlog::debug(
                "Sending the get request with id {} to built",
                (void*) wait_built->request.get()
              );

              worker_queue.push(
                std::make_unique<::mhconfig::worker::command::BuildCommand>(
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

  if (wait_build_->template_ != nullptr) {
    spdlog::debug("Setting the template '{}'", wait_build_->request->template_());

    auto wait_builts_search = config_namespace.wait_builts_by_key
      .find(wait_build_->overrides_key);

    // TODO Cache also if the template isn't ok to avoid try render this again
    if (wait_build_->is_template_ok) {
      auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
        config_namespace,
        wait_build_->overrides_key
      );
      merged_config->status = MergedConfigStatus::OK_TEMPLATE;
      merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();
      merged_config->preprocesed_value = wait_build_->template_rendered;

      if (wait_builts_search != config_namespace.wait_builts_by_key.end()) {
        for (size_t i = wait_builts_search->second.size(); i--; ) {
          auto wait_built = wait_builts_search->second[i].get();

          spdlog::debug(
            "Responding the get request with id {}",
            (void*) wait_built->request.get()
          );

          worker_queue.push(
            std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
              std::move(wait_built->request),
              merged_config
            )
          );
        }

        config_namespace.wait_builts_by_key.erase(wait_builts_search);
      }
    } else {
      if (wait_builts_search != config_namespace.wait_builts_by_key.end()) {
        for (size_t i = wait_builts_search->second.size(); i--; ) {
          auto wait_built = wait_builts_search->second[i].get();

          spdlog::debug(
            "Responding the get request with id {}",
            (void*) wait_built->request.get()
          );

          wait_build_->request->set_status(
            ::mhconfig::api::request::GetRequest::Status::ERROR
          );

          worker_queue.push(
            std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
              std::move(wait_build_->request)
            )
          );
        }

        config_namespace.wait_builts_by_key.erase(wait_builts_search);
      }
    }
  }

  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    wait_build_->overrides_key
  );

  if ((wait_build_->template_ == nullptr) || wait_build_->is_template_ok) {
    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
        std::move(wait_build_->request),
        std::move(merged_config)
      )
    );
  } else {
    wait_build_->request->set_status(
      ::mhconfig::api::request::GetRequest::Status::ERROR
    );

    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
        std::move(wait_build_->request)
      )
    );
  }

  return NamespaceExecutionResult::OK;
}

bool SetDocumentsCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  wait_build_->request->set_element(UNDEFINED_ELEMENT.get());

  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(wait_build_->request)
    )
  );

  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
