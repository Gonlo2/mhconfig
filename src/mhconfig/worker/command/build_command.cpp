#include "mhconfig/worker/command/build_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

BuildCommand::BuildCommand(
  uint64_t namespace_id,
  std::shared_ptr<string_pool::Pool> pool,
  std::shared_ptr<build::wait_built_t> wait_build
)
  : Command(),
  namespace_id_(namespace_id),
  pool_(pool),
  wait_build_(wait_build)
{
}

BuildCommand::~BuildCommand() {
}

std::string BuildCommand::name() const {
  return "BUILD";
}

bool BuildCommand::execute(
  Queue<scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) {
  std::unordered_map<
    std::string,
    build::built_element_t
  > built_elements_by_document;

  std::unordered_map<std::string, ElementRef> ref_elements_by_document;
  for (auto& build_element : wait_build_->elements_to_build) {
    size_t override_id = 0;
    ElementRef config = build_element.config;

    if (config == nullptr) {
      while ((override_id < wait_build_->request->overrides().size()) && (config == nullptr)) {
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

      config = (config == nullptr)
        ? UNDEFINED_ELEMENT
        : mhconfig::builder::apply_tags(pool_, config, config, ref_elements_by_document);

      command::build::built_element_t built_element;
      built_element.overrides_key = build_element.overrides_key;
      built_element.config = config;

      built_elements_by_document[build_element.name] = built_element;
    }

    ref_elements_by_document[build_element.name] = config;
  }

  //scheduler_queue_.push(command);

  //auto add_namespace_command = std::make_shared<scheduler::command::AddNamespaceCommand>(config_namespace);
  //scheduler_queue.push(add_namespace_command);
  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
