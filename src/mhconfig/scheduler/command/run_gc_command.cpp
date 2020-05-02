#include "mhconfig/scheduler/command/run_gc_command.h"

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

RunGcCommand::RunGcCommand(
  run_gc::Type type,
  uint32_t max_live_in_seconds
) : Command(),
    type_(type),
    max_live_in_seconds_(max_live_in_seconds)
{
}

RunGcCommand::~RunGcCommand() {
}

std::string RunGcCommand::name() const {
  return "API_RUN_GC";
}

CommandType RunGcCommand::command_type() const {
  return CommandType::GENERIC;
}

bool RunGcCommand::execute(
  scheduler_context_t& context
) {
  switch (type_) {
    case run_gc::Type::CACHE_GENERATION_0:
      remove_merge_configs(context, 0);
      break;
    case run_gc::Type::CACHE_GENERATION_1:
      remove_merge_configs(context, 1);
      break;
    case run_gc::Type::CACHE_GENERATION_2:
      remove_merge_configs(context, 2);
      break;
    case run_gc::Type::DEAD_POINTERS:
      remove_dead_pointers(context);
      break;
    case run_gc::Type::NAMESPACES:
      remove_namespaces(context);
      break;
    case run_gc::Type::VERSIONS:
      remove_versions(context);
      break;
  }

  return true;
}

void RunGcCommand::remove_merge_configs(
  scheduler_context_t& context,
  uint32_t generation
) {
  spdlog::debug(
    "To remove merge configs in the {} generation without accessing for {} sec",
    generation,
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::time::monotonic_now_sec();

  size_t number_of_removed_merged_configs = 0;
  size_t number_of_processed_merged_configs = 0;
  for (auto& it : context.namespace_by_path) {
    auto &from = it.second->merged_config_by_gc_generation[generation];
    number_of_processed_merged_configs += from.size();

    if (generation == 0) {
      // This generation has the young configs, they has some properties:
      // - If the config is undefined or building the config remain here
      // - They use the api basic merged config to avoid waste CPU optimizing it
      // - Once the config is promoted to the next generation a worker command optimize it in background

      auto& to = it.second->merged_config_by_gc_generation[1];
      to.reserve(to.size() + from.size());
      for (size_t i = 0; i < from.size(); ) {
        if(
          (from[i]->status == MergedConfigStatus::OK)
          && (from[i]->creation_timestamp + max_live_in_seconds_ <= current_timestamp)
        ) {
          if (from[i]->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
            ++number_of_removed_merged_configs;
          } else {
            auto optimize_merged_config_command = std::make_shared<::mhconfig::worker::command::OptimizeMergedConfigCommand>(
              from[i],
              it.second->pool
            );
            context.worker_queue.push(optimize_merged_config_command);

            to.push_back(from[i]);
          }
          from[i] = from.back();
          from.pop_back();
        } else {
          ++i;
        }
      }
    } else if (generation == 1) {
      // This generation has the senior configs
      auto& to = it.second->merged_config_by_gc_generation[2];
      to.reserve(to.size() + from.size());

      for (auto merged_config : from) {
        if(merged_config->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
          ++number_of_removed_merged_configs;
        } else {
          to.push_back(merged_config);
        }
      }

      from.clear();
    } else if (generation == 2) {
      // This generation has the guru configs
      for (size_t i = 0; i < from.size(); ) {
        if(from[i]->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
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

void RunGcCommand::remove_dead_pointers(
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

void RunGcCommand::remove_namespaces(
  scheduler_context_t& context
) {
  spdlog::debug(
    "To remove namespaces without accessing for {} sec",
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::time::monotonic_now_sec();

  size_t number_of_removed_namespaces = 0;
  size_t number_of_processed_namespaces = context.namespace_by_id.size();
  for (
    auto it = context.namespace_by_id.begin();
    it != context.namespace_by_id.end();
  ) {
    if (it->second->last_access_timestamp + max_live_in_seconds_ <= current_timestamp) {
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


void RunGcCommand::remove_versions(
  scheduler_context_t& context
) {
  spdlog::debug(
    "To remove versions without accessing for {} sec",
    max_live_in_seconds_
  );

  uint64_t current_timestamp = jmutils::time::monotonic_now_sec();

  for (auto& it : context.namespace_by_path) {
    auto config_namespace = it.second;

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
