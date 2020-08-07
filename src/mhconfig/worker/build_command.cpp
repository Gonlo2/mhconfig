#include "mhconfig/worker/build_command.h"

namespace mhconfig
{
namespace worker
{

BuildCommand::BuildCommand(
  uint64_t namespace_id,
  std::shared_ptr<jmutils::string::Pool> pool,
  std::shared_ptr<build::wait_built_t>&& wait_build
)
  : namespace_id_(namespace_id),
  pool_(pool),
  wait_build_(std::move(wait_build))
{
}

BuildCommand::~BuildCommand() {
}

std::string BuildCommand::name() const {
  return "BUILD";
}

bool BuildCommand::force_take_metric() const {
  return true;
}

bool BuildCommand::execute(
  context_t& context
) {
  absl::flat_hash_map<std::string, Element> ref_elements_by_document;
  for (auto& build_element : wait_build_->elements_to_build) {
    spdlog::debug("Building the document '{}'", build_element.name);

    if (build_element.to_build) {
      bool first_config = true;
      Element config;

      builder::for_each_document_override_path(
        wait_build_->request->flavors(),
        wait_build_->request->overrides(),
        build_element.name,
        [this, &first_config, &config, &ref_elements_by_document](const auto& override_path) {
          auto search = wait_build_->raw_config_by_override_path
            .find(override_path);
          if (search != wait_build_->raw_config_by_override_path.end()) {
            if (first_config) {
              config = search->second->value;
              first_config = false;
            } else {
              config = builder::override_with(
                config,
                search->second->value,
                ref_elements_by_document
              );
            }
          }
        }
      );

      if (!config.is_undefined()) {
        build_element.config = mhconfig::builder::apply_tags(
          pool_.get(),
          config,
          config,
          ref_elements_by_document
        ).second;
      }

      build_element.config.freeze();
    }

    ref_elements_by_document[build_element.name] = build_element.config;
  }

  proto::GetResponse get_response;
  auto checksum = wait_build_->elements_to_build.back().config.make_checksum();
  get_response.set_checksum(checksum.data(), checksum.size());
  api::config::fill_elements(
    wait_build_->elements_to_build.back().config,
    &get_response,
    get_response.add_elements()
  );

  wait_build_->is_preprocesed_value_ok = get_response.SerializeToString(
    &wait_build_->preprocesed_value
  );
  if (wait_build_->is_preprocesed_value_ok) {
    context.async_metrics_service->add(
      metrics::MetricsService::MetricId::OPTIMIZED_MERGED_CONFIG_USED_BYTES,
      {},
      wait_build_->preprocesed_value.size()
    );
  } else {
    spdlog::warn("Can't optimize the config of the document");
  }

  context.scheduler_queue->push(
    std::make_unique<::mhconfig::scheduler::SetDocumentsCommand>(
      namespace_id_,
      std::move(wait_build_)
    )
  );

  return true;
}

} /* worker */
} /* mhconfig */
