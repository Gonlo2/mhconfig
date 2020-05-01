#include "mhconfig/scheduler/command/api_run_gc_command.h"

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/command/update_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ApiRunGCCommand::ApiRunGCCommand(
  ::mhconfig::api::request::run_gc::Type type,
  uint32_t max_live_in_seconds
) : Command(),
    type_(type),
    max_live_in_seconds_(max_live_in_seconds)
{
}

ApiRunGCCommand::~ApiRunGCCommand() {
}

std::string ApiRunGCCommand::name() const {
  return "API_RUN_GC";
}

CommandType ApiRunGCCommand::command_type() const {
  return CommandType::GENERIC;
}

bool ApiRunGCCommand::execute(
  scheduler_context_t& context
) {
  uint64_t limit_timestamp = jmutils::time::monotonic_now_sec() - max_live_in_seconds_;

  switch (type_) {
    case ::mhconfig::api::request::run_gc::Type::CACHE_GENERATION_0:
      remove_merge_configs(context, limit_timestamp, 0);
      break;
    case ::mhconfig::api::request::run_gc::Type::CACHE_GENERATION_1:
      remove_merge_configs(context, limit_timestamp, 1);
      break;
    case ::mhconfig::api::request::run_gc::Type::CACHE_GENERATION_2:
      remove_merge_configs(context, limit_timestamp, 2);
      break;
    case ::mhconfig::api::request::run_gc::Type::DEAD_POINTERS:
      remove_dead_pointers(context);
      break;
    case ::mhconfig::api::request::run_gc::Type::NAMESPACES:
      remove_namespaces(context, limit_timestamp);
      break;
    case ::mhconfig::api::request::run_gc::Type::VERSIONS:
      remove_versions(context, limit_timestamp);
      break;
  }

  return true;
}

void ApiRunGCCommand::remove_merge_configs(
  scheduler_context_t& context,
  uint32_t limit_timestamp,
  uint32_t generation
) {
  spdlog::debug(
    "To remove merge configs in the {} generation without accessing them since timestamp {}",
    generation,
    limit_timestamp
  );

  size_t number_of_removed_merged_configs = 0;
  size_t number_of_processed_merged_configs = 0;
  for (auto& it : context.namespace_by_path) {
    auto &from = it.second->merged_config_by_gc_generation[generation];
    number_of_processed_merged_configs += from.size();

    if (generation+1 < NUMBER_OF_GC_GENERATIONS) {
      auto& to = it.second->merged_config_by_gc_generation[generation+1];
      to.reserve(to.size() + from.size());

      for (auto merged_config : from) {
        if (merged_config->last_access_timestamp <= limit_timestamp) {
          ++number_of_removed_merged_configs;
        } else {
          to.push_back(merged_config);
        }
      }

      from.clear();
    } else {
      for (size_t i = 0; i < from.size(); ) {
        if (from[i]->last_access_timestamp < limit_timestamp) {
          from[i] = from.back();
          from.pop_back();
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

void ApiRunGCCommand::remove_dead_pointers(
  scheduler_context_t& context
) {
  spdlog::debug("To remove dead pointers");

  size_t number_of_removed_dead_pointers = 0;
  size_t number_of_processed_pointers = 0;
  for (auto& it : context.namespace_by_path) {
    for (auto& it_2 : it.second->merged_config_metadata_by_overrides_key) {
      number_of_processed_pointers += it_2.second->merged_config_by_document.size();

      for (
        auto it_3 = it_2.second->merged_config_by_document.begin();
        it_3 != it_2.second->merged_config_by_document.end();
      ) {
        if (it_3->second.expired()) {
          it_3 = it_2.second->merged_config_by_document.erase(it_3);
          ++number_of_removed_dead_pointers;
        } else {
          ++it_3;
        }
      }
    }
  }

  spdlog::debug(
    "Removed dead pointers (removed: {}, processed: {})",
    number_of_removed_dead_pointers,
    number_of_processed_pointers
  );
}

void ApiRunGCCommand::remove_namespaces(
  scheduler_context_t& context,
  uint32_t limit_timestamp
) {
  spdlog::debug(
    "To remove namespaces without accessing them since timestamp {}",
    limit_timestamp
  );

  size_t number_of_removed_namespaces = 0;
  size_t number_of_processed_namespaces = context.namespace_by_id.size();
  for (
    auto it = context.namespace_by_id.begin();
    it != context.namespace_by_id.end();
  ) {
    if (it->second->last_access_timestamp <= limit_timestamp) {
      spdlog::debug(
        "Removing the namespace '{}' with id {}",
        it->second->root_path,
        it->first
      );

      auto search = context.namespace_by_path.find(it->second->root_path);
      if (
        (search != context.namespace_by_path.end())
        && (search->second->id == it->first)
      ) {
        context.namespace_by_path.erase(search);
      }

      it = context.namespace_by_id.erase(it);
      ++number_of_removed_namespaces;
    } else {
      ++it;
    }
  }

  spdlog::debug(
    "Removed namespaces (removed: {}, processed: {})",
    number_of_removed_namespaces,
    number_of_processed_namespaces
  );
}


void ApiRunGCCommand::remove_versions(
  scheduler_context_t& context,
  uint32_t limit_timestamp
) {
  spdlog::debug(
    "To remove versions deprecated before timestamp {}",
    limit_timestamp
  );

  for (auto& it : context.namespace_by_path) {
    auto config_namespace = it.second;

    if (
        (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
        && (config_namespace->stored_versions_by_deprecation_timestamp.front().first <= limit_timestamp)
    ) {
      while (
          (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
          && (config_namespace->stored_versions_by_deprecation_timestamp.front().first <= limit_timestamp)
      ) {
        config_namespace->stored_versions_by_deprecation_timestamp.pop_front();
      }

      uint32_t remove_till_version = config_namespace
        ->stored_versions_by_deprecation_timestamp
        .front()
        .second;

      for (
        auto it_2 = config_namespace->document_metadata_by_document.begin();
        it_2 != config_namespace->document_metadata_by_document.end();
      ) {
        auto& raw_config_by_version_by_override = it_2
          ->second
          ->raw_config_by_version_by_override;

        for (
          auto it_3 = raw_config_by_version_by_override.begin();
          it_3 != raw_config_by_version_by_override.end();
        ) {
          auto& raw_config_by_version = it_3->second;

          auto version_search = raw_config_by_version.lower_bound(remove_till_version);
          bool delete_override = version_search == raw_config_by_version.end();
          if (delete_override) {
            delete_override = raw_config_by_version.rbegin()->second->value == nullptr;
            if (!delete_override) --version_search;
          }

          if (delete_override) {
            spdlog::debug(
              "Removed override '{}' in the namespace '{}'",
              it_3->first,
              it.first
            );

            it_3 = raw_config_by_version_by_override.erase(it_3);
          } else {
            spdlog::debug(
              "Removed the {} previous versions to {} of the override '{}' in the namespace '{}'",
              std::distance(raw_config_by_version.begin(), version_search),
              remove_till_version,
              it_3->first,
              it.first
            );

            raw_config_by_version.erase(
              raw_config_by_version.begin(),
              version_search
            );
            ++it_3;
          }
        }

        if (raw_config_by_version_by_override.empty()) {
          it_2 = config_namespace->document_metadata_by_document.erase(it_2);
        } else {
          ++it_2;
        }
      }
    }
  }
}

} /* command */
} /* scheduler */
} /* mhconfig */
