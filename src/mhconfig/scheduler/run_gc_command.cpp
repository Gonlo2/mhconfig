#include "mhconfig/scheduler/run_gc_command.h"

namespace mhconfig
{
namespace scheduler
{

RunGcCommand::RunGcCommand(
  Type type,
  uint32_t max_live_in_seconds
) : SchedulerCommand(),
    type_(type),
    max_live_in_seconds_(max_live_in_seconds)
{
}

RunGcCommand::~RunGcCommand() {
}

std::string RunGcCommand::name() const {
  return "API_RUN_GC";
}

SchedulerCommand::CommandType RunGcCommand::type() const {
  return CommandType::GENERIC;
}

bool RunGcCommand::execute(
  context_t& context
) {
  switch (type_) {
    case Type::CACHE_GENERATION_0:
      remove_merge_configs(context, 0);
      break;
    case Type::CACHE_GENERATION_1:
      remove_merge_configs(context, 1);
      break;
    case Type::CACHE_GENERATION_2:
      remove_merge_configs(context, 2);
      break;
    case Type::DEAD_POINTERS:
      remove_dead_pointers(context);
      break;
    case Type::NAMESPACES:
      remove_namespaces(context);
      break;
    case Type::VERSIONS:
      remove_versions(context);
      break;
  }

  return true;
}

void RunGcCommand::remove_merge_configs(
  context_t& context,
  uint32_t generation
) {
  spdlog::debug(
    "To remove merge configs in the {} generation without accessing for {} sec",
    generation,
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::monotonic_now_sec();

  size_t number_of_removed_merged_configs = 0;
  size_t number_of_processed_merged_configs = 0;
  for (auto& it : context.namespace_by_path) {
    auto &from = it.second->merged_config_by_gc_generation[generation];
    number_of_processed_merged_configs += from.size();

    if (generation == 0) {
      // This generation has the young configs
      auto& to = it.second->merged_config_by_gc_generation[1];
      to.reserve(to.size() + from.size());
      for (size_t i = 0; i < from.size(); ) {
        if(
          ((from[i]->status == MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED)
            || (from[i]->status == MergedConfigStatus::OK_CONFIG_OPTIMIZING)
            || (from[i]->status == MergedConfigStatus::OK_CONFIG_OPTIMIZED)
            || (from[i]->status == MergedConfigStatus::OK_TEMPLATE)
          ) && (from[i]->creation_timestamp + max_live_in_seconds_ <= current_timestamp)
        ) {
          if (from[i]->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
            ++number_of_removed_merged_configs;
          } else {
            to.push_back(std::move(from[i]));
          }
          jmutils::swap_delete(from, i);
        } else {
          ++i;
        }
      }
    } else if (generation == 1) {
      // This generation has the senior configs
      auto& to = it.second->merged_config_by_gc_generation[2];
      to.reserve(to.size() + from.size());

      for (size_t i = from.size(); i--;) {
        if(from[i]->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
          ++number_of_removed_merged_configs;
        } else {
          to.push_back(std::move(from[i]));
        }
      }

      from.clear();
    } else if (generation == 2) {
      // This generation has the guru configs
      for (size_t i = 0; i < from.size(); ) {
        if(from[i]->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
          jmutils::swap_delete(from, i);
          ++number_of_removed_merged_configs;
        } else {
          ++i;
        }
      }
    }
  }

  spdlog::debug(
    "Removed merged configs (removed: {}, processed: {})",
    number_of_removed_merged_configs,
    number_of_processed_merged_configs
  );
}

void RunGcCommand::remove_dead_pointers(
  context_t& context
) {
  spdlog::debug("To remove dead pointers");

  std::vector<std::string> overrides_key_to_remove;
  std::vector<std::string> to_remove_tmp;

  for (auto& it : context.namespace_by_path) {
    jmutils::remove_expired(it.second->watchers);

    jmutils::remove_expired_map_values(it.second->traces_by_override);
    jmutils::remove_expired_map_values(it.second->traces_by_flavor);
    jmutils::remove_expired_map_values(it.second->traces_by_document);
    jmutils::remove_expired(it.second->to_trace_always);

    jmutils::remove_expired_map(it.second->merged_config_by_overrides_key);
  }
}

void RunGcCommand::remove_namespaces(
  context_t& context
) {
  spdlog::debug(
    "To remove namespaces without accessing for {} sec",
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::monotonic_now_sec();
  std::vector<uint64_t> namespaces_to_remove;

  size_t number_of_processed_namespaces = context.namespace_by_id.size();
  for (auto it: context.namespace_by_id) {
    spdlog::trace(
      "Checking the namespace '{}' with id {} (timestamp: {}, num_watchers: {})",
      it.second->root_path,
      it.first,
      it.second->last_access_timestamp,
      it.second->watchers.size()
    );

    if (
      (it.second->last_access_timestamp + max_live_in_seconds_ <= current_timestamp)
      && !is_namespace_in_use(*it.second)
    ) {
      spdlog::debug(
        "Removing the namespace '{}' with id {}",
        it.second->root_path,
        it.first
      );

      auto search = context.namespace_by_path.find(it.second->root_path);
      if (
        (search != context.namespace_by_path.end())
        && (search->second->id == it.first)
      ) {
        context.namespace_by_path.erase(search);
      }

      namespaces_to_remove.push_back(it.first);
    }
  }

  for (uint64_t id : namespaces_to_remove) {
    context.namespace_by_id.erase(id);
  }

  spdlog::debug(
    "Removed namespaces (removed: {}, processed: {})",
    namespaces_to_remove.size(),
    number_of_processed_namespaces
  );
}

void RunGcCommand::remove_versions(
  context_t& context
) {
  spdlog::debug(
    "To remove versions without accessing for {} sec",
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::monotonic_now_sec();

  std::vector<std::string> override_paths_to_remove;

  for (auto& namespace_it : context.namespace_by_path) {
    auto config_namespace = namespace_it.second;

    if (
      (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
      && (config_namespace->stored_versions_by_deprecation_timestamp.front().first + max_live_in_seconds_ <= current_timestamp)
    ) {
      while (
        (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
        && (config_namespace->stored_versions_by_deprecation_timestamp.front().first + max_live_in_seconds_ <= current_timestamp)
      ) {
        config_namespace->stored_versions_by_deprecation_timestamp.pop_front();
      }

      uint32_t remove_till_version = config_namespace
        ->stored_versions_by_deprecation_timestamp
        .front()
        .second;

      override_paths_to_remove.clear();
      for (auto override_metadata_it: config_namespace->override_metadata_by_override_path) {
        jmutils::remove_expired(override_metadata_it.second.watchers);

        auto& raw_config_by_version = override_metadata_it.second.raw_config_by_version;

        auto it = raw_config_by_version.begin();
        while (
          (raw_config_by_version.size() > 1) && (it->first < remove_till_version)
        ) {
          spdlog::debug(
            "Removed the version {} of the override_path '{}' in the namespace '{}'",
            it->first,
            override_metadata_it.first,
            namespace_it.first
          );
          it = raw_config_by_version.erase(it);
        }

        if (!raw_config_by_version.empty() && (it->second == nullptr)) {
          spdlog::debug(
            "Removed the version {} of the override_path '{}' in the namespace '{}'",
            it->first,
            override_metadata_it.first,
            namespace_it.first
          );
          it = raw_config_by_version.erase(it);
        }

        if (raw_config_by_version.empty() && override_metadata_it.second.watchers.empty()) {
          spdlog::debug(
            "Removed override_path '{}' in the namespace '{}'",
            override_metadata_it.first,
            namespace_it.first
          );
          override_paths_to_remove.push_back(override_metadata_it.first);
        }
      }

      for (const auto& k : override_paths_to_remove) {
        config_namespace->override_metadata_by_override_path.erase(k);
      }
    }
  }
}

} /* scheduler */
} /* mhconfig */
