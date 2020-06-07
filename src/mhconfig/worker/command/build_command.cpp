#include "mhconfig/worker/command/build_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

bool fill_json(
  const mhconfig::Element& root,
  nlohmann::json& output
) {
  switch (root.type()) {
    case NodeType::UNDEFINED_NODE:
      return false;

    case NodeType::NULL_NODE:
    case NodeType::OVERRIDE_NULL_NODE:
      return true;

    case NodeType::STR_NODE: // Fallback
    case NodeType::OVERRIDE_STR_NODE: {
      auto r = root.try_as<std::string>();
      output = r.second;
      return r.first;
    }

    case NodeType::INT_NODE: // Fallback
    case NodeType::OVERRIDE_INT_NODE: {
      auto r = root.try_as<int64_t>();
      output = r.second;
      return r.first;
    }

    case NodeType::FLOAT_NODE: // Fallback
    case NodeType::OVERRIDE_FLOAT_NODE: {
      auto r = root.try_as<double>();
      output = r.second;
      return r.first;
    }

    case NodeType::BOOL_NODE: // Fallback
    case NodeType::OVERRIDE_BOOL_NODE: {
      auto r = root.try_as<bool>();
      output = r.second;
      return r.first;
    }

    case NodeType::MAP_NODE: // Fallback
    case NodeType::OVERRIDE_MAP_NODE: {
      for (const auto& it : *root.as_map()) {
        if (!fill_json(it.second, output[it.first.str()])) {
          return false;
        }
      }
      return true;
    }

    case NodeType::SEQUENCE_NODE: // Fallback
    case NodeType::FORMAT_NODE: // Fallback
    case NodeType::SREF_NODE: // Fallback
    case NodeType::REF_NODE: // Fallback
    case NodeType::OVERRIDE_SEQUENCE_NODE: {
      auto seq = root.as_sequence();
      std::vector<nlohmann::json> values(seq->size());
      for (size_t i = seq->size(); i--;) {
        if (!fill_json((*seq)[i], values[i])) {
          return false;
        }
      }
      output = std::move(values);
      return true;
    }
  }

  return false;
}

BuildCommand::BuildCommand(
  uint64_t namespace_id,
  std::shared_ptr<::string_pool::Pool> pool,
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
      size_t override_id = 0;
      Element config;
      while ((override_id < wait_build_->request->overrides().size()) && config.is_undefined()) {
        auto search = build_element.raw_config_by_override
          .find(wait_build_->request->overrides()[override_id]);
        if (search != build_element.raw_config_by_override.end()) {
          config = search->second->value;
        }
        ++override_id;
      }

      while (override_id < wait_build_->request->overrides().size()) {
        auto search = build_element.raw_config_by_override
          .find(wait_build_->request->overrides()[override_id]);
        if (search != build_element.raw_config_by_override.end()) {
          config = mhconfig::builder::override_with(
            config,
            search->second->value,
            ref_elements_by_document
          );
        }
        ++override_id;
      }

      if (!config.is_undefined()) {
        mhconfig::builder::apply_tags(
          pool_.get(),
          config,
          config,
          ref_elements_by_document,
          build_element.config
        );
      }
    }

    ref_elements_by_document[build_element.name] = build_element.config;
  }

  if (wait_build_->template_ == nullptr) {
    ::mhconfig::proto::GetResponse get_response;
    ::mhconfig::api::config::fill_elements(
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
  } else {
    wait_build_->is_preprocesed_value_ok = false;
    try {
      nlohmann::json data;
      if (fill_json(wait_build_->elements_to_build.back().config, data)) {
        inja::TemplateStorage included_templates;
        inja::FunctionStorage callbacks;

        std::stringstream os;
        inja::Renderer renderer(included_templates, callbacks);
        renderer.render_to(os, *wait_build_->template_, data);

        wait_build_->preprocesed_value = std::move(os.str());
        wait_build_->is_preprocesed_value_ok = true;
      }
    } catch(const std::exception &e) {
      spdlog::error(
        "Error rendering the template: {}",
        e.what()
      );
    } catch(...) {
      spdlog::error("Unknown error rendering the template");
    }
  }

  context.scheduler_queue->push(
    std::make_unique<::mhconfig::scheduler::command::SetDocumentsCommand>(
      namespace_id_,
      std::move(wait_build_)
    )
  );

  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
