#include "mhconfig/scheduler/command/run_gc_command.h"

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/command/update_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"
#include "jmutils/common.h"

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
          jmutils::swap_delete(from, i);
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
  scheduler_context_t& context
) {
  spdlog::debug("To remove dead pointers");

  size_t number_of_removed_dead_pointers = 0;
  size_t number_of_processed_pointers = 0;
  for (auto& it : context.namespace_by_path) {
    number_of_processed_pointers += it.second
      ->merged_config_by_overrides_key.size();

    for (
      auto it_2 = it.second->merged_config_by_overrides_key.begin();
      it_2 != it.second->merged_config_by_overrides_key.end();
    ) {
      if (it_2->second.expired()) {
        it_2 = it.second->merged_config_by_overrides_key.erase(it_2);
        ++number_of_removed_dead_pointers;
      } else {
        ++it_2;
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
    if (
      (it->second->last_access_timestamp + max_live_in_seconds_ <= current_timestamp)
      && (it->second->num_watchers == 0)
    ) {
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
        auto& override_by_key = it_2->second->override_by_key;
        for (
          auto it_3 = override_by_key.begin();
          it_3 != override_by_key.end();
        ) {
          auto& watchers = it_3->second.watchers;
          //TODO add some stats
          for (size_t i = 0; i < watchers.size();) {
            if (watchers[i].expired()) {
              jmutils::swap_delete(watchers, i);
              --(config_namespace->num_watchers);
            } else {
              ++i;
            }
          }

          auto& raw_config_by_version = it_3->second.raw_config_by_version;

          auto it_4 = raw_config_by_version.begin();
          while (
            (raw_config_by_version.size() > 1) && (it_4->first < remove_till_version)
          ) {
            spdlog::debug(
              "Removed the version {} of the document '{}' with override '{}' in the namespace '{}'",
              it_4->first,
              it_2->first,
              it_3->first,
              it.first
            );
            it_4 = raw_config_by_version.erase(it_4);
          }

          if (!raw_config_by_version.empty() && (it_4->second->value == nullptr)) {
            spdlog::debug(
              "Removed the version {} of the document '{}' with override '{}' in the namespace '{}'",
              it_4->first,
              it_2->first,
              it_3->first,
              it.first
            );
            it_4 = raw_config_by_version.erase(it_4);
          }

          if (raw_config_by_version.empty() && it_3->second.watchers.empty()) {
            spdlog::debug(
              "Removed override '{}' in the namespace '{}'",
              it_3->first,
              it.first
            );
            it_3 = override_by_key.erase(it_3);
          } else {
            ++it_3;
          }
        }

        if (override_by_key.empty()) {
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
