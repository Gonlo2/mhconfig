#include "mhconfig/scheduler/update_documents_command.h"

namespace mhconfig
{
namespace scheduler
{

UpdateDocumentsCommand::UpdateDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request,
  absl::flat_hash_map<std::string, load_raw_config_result_t>&& items
)
  : SchedulerCommand(),
  namespace_id_(namespace_id),
  update_request_(update_request),
  items_(std::move(items))
{
}

UpdateDocumentsCommand::~UpdateDocumentsCommand() {
}

std::string UpdateDocumentsCommand::name() const {
  return "UPDATE_DOCUMENTS";
}

SchedulerCommand::CommandType UpdateDocumentsCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_ID;
}

uint64_t UpdateDocumentsCommand::namespace_id() const {
  return namespace_id_;
}

SchedulerCommand::CommandResult UpdateDocumentsCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  CommandResult result = CommandResult::OK;

  update_request_->set_namespace_id(config_namespace.id);

  std::vector<std::pair<std::string, load_raw_config_result_t>> extra_items;
  if (update_request_->reload()) {
    spdlog::debug("Obtaining the existing documents to remove");
    add_items_to_remove(config_namespace, extra_items);
  }

  spdlog::debug("Filtering the existing documents");
  filter_existing_documents(config_namespace);

  items_.reserve(items_.size() + extra_items.size());
  for (size_t i = 0, l = extra_items.size(); i < l; ++i) {
    items_.emplace(std::move(extra_items[i]));
  }

  if (items_.empty()) {
    spdlog::debug("Without items to update");
  } else {
    spdlog::debug(
      "Increasing the current version {} of the namespace with ID {}",
      config_namespace.current_version,
      config_namespace.id
    );
    ++(config_namespace.current_version);


    config_namespace.stored_versions_by_deprecation_timestamp
      .back()
      .first = jmutils::monotonic_now_sec();

    config_namespace.stored_versions_by_deprecation_timestamp
      .emplace_back(0, config_namespace.current_version);


    spdlog::debug("Decreasing the referenced_by counter of the previous raw config");
    decrease_references(config_namespace);

    absl::flat_hash_set<std::shared_ptr<api::stream::WatchInputMessage>> watchers_to_trigger;

    spdlog::debug("Updating the ids of the affected documents");

    absl::flat_hash_map<
      std::pair<std::string, std::string>,
      absl::flat_hash_map<std::string, builder::AffectedDocumentStatus>
    > updated_documents_by_flavor_and_override;

    // If some config will be removed and that document use a reference to another
    // modified document, we need to create a empty version to invalidate the cache
    for (auto& it : items_) {
      auto key = std::make_pair(it.second.flavor, it.second.override_);
      auto status = (it.second.status == LoadRawConfigStatus::FILE_DONT_EXISTS)
        ? builder::AffectedDocumentStatus::TO_REMOVE
        : builder::AffectedDocumentStatus::TO_ADD;

      updated_documents_by_flavor_and_override[key].emplace(
        it.second.document,
        status
      );
    }

    builder::increment_version_of_the_affected_documents(
      config_namespace,
      updated_documents_by_flavor_and_override,
      watchers_to_trigger,
      false
    );

    spdlog::debug("Inserting the elements of the updated documents");
    insert_updated_documents(config_namespace, watchers_to_trigger);

    for (auto& watcher : watchers_to_trigger) {
      spdlog::debug(
        "The document '{}' changed and it has a watcher",
        watcher->document()
      );

      scheduler_queue.push(
        std::make_unique<scheduler::ApiGetCommand>(
          std::make_shared<::mhconfig::api::stream::WatchGetRequest>(
            watcher,
            watcher->make_output_message()
          )
        )
      );
    }

    if (
      (config_namespace.next_raw_config_id >= 0xff000000) // TODO check a better way to do this
      || (config_namespace.current_version >= 0xfffffff0)
    ) {
      spdlog::warn(
        "Softdeleting the namespace '{}' because the internal ids are in the limit",
        config_namespace.root_path
      );
      result = CommandResult::SOFTDELETE_NAMESPACE;
    }
  }

  update_request_->set_status(::mhconfig::api::request::UpdateRequest::Status::OK);
  update_request_->set_version(config_namespace.current_version);
  send_api_response(worker_queue);

  return result;
}

bool UpdateDocumentsCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  update_request_->set_status(::mhconfig::api::request::UpdateRequest::Status::ERROR);
  send_api_response(worker_queue);

  return true;
}

void UpdateDocumentsCommand::send_api_response(
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(update_request_)
    )
  );
}

void UpdateDocumentsCommand::add_items_to_remove(
  config_namespace_t& config_namespace,
  std::vector<std::pair<std::string, load_raw_config_result_t>>& result
) {
  for (const auto it: config_namespace.override_metadata_by_override_path) {
    if (has_last_version(it.second) && (items_.count(it.first) == 0)) {
      spdlog::debug("Adding the override path '{}' to remove", it.first);

      auto split_result = split_override_path(it.first);

      load_raw_config_result_t item;
      item.status = LoadRawConfigStatus::FILE_DONT_EXISTS;
      item.override_ = split_result.override_;
      item.document = split_result.document;
      item.flavor = split_result.flavor;
      item.raw_config = nullptr;

      result.emplace_back(it.first, std::move(item));
    }
  }
}

void UpdateDocumentsCommand::filter_existing_documents(
  config_namespace_t& config_namespace
) {
  std::vector<std::string> override_paths_to_remove;

  for (const auto& it: items_) {
    if (it.second.status == LoadRawConfigStatus::OK) {
      uint32_t new_crc32 = it.second.raw_config->crc32;
      with_raw_config(
        config_namespace,
        it.first,
        0,
        [&override_paths_to_remove, new_crc32](const auto& override_path, const auto& raw_config) {
          if (raw_config->has_content && (raw_config->crc32 == new_crc32)) {
            spdlog::debug(
              "Filtering the existing raw config (override_path: '{}', crc32: {:X})",
              override_path,
              new_crc32
            );
            override_paths_to_remove.push_back(override_path);
          }
        }
      );
    }
  }

  for (const auto& override_path : override_paths_to_remove) {
    items_.erase(override_path);
  }
}

void UpdateDocumentsCommand::decrease_references(
  config_namespace_t& config_namespace
) {
  for (auto& it : items_) {
    with_raw_config(
      config_namespace,
      it.first,
      0,
      [&config_namespace, &it](const auto& override_path, const auto& raw_config) {
        if (raw_config->has_content) {
          for (const auto& reference_to : raw_config->reference_to) {
            auto& referenced_by = config_namespace
              .referenced_by_by_document[reference_to];

            spdlog::debug(
              "Decreasing referenced_by counter (override_path: '{}', reference_to: '{}', counter: {})",
              override_path,
              reference_to,
              referenced_by[it.second.document].v
            );

            if (referenced_by[it.second.document].v-- <= 1) {
              referenced_by.erase(it.second.document);
            }
          }
        }
      }
    );
  }
}

void UpdateDocumentsCommand::insert_updated_documents(
  config_namespace_t& config_namespace,
  absl::flat_hash_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>>& watchers_to_trigger
) {
  for (auto& it : items_) {
    auto& override_metadata = config_namespace.override_metadata_by_override_path[it.first];

    if (it.second.status == LoadRawConfigStatus::FILE_DONT_EXISTS) {
      spdlog::debug("Removing raw config if possible (override_path: '{}')", it.first);

      override_metadata.raw_config_by_version.try_emplace(
        config_namespace.current_version,
        nullptr
      );
    } else {
      for (const auto& reference_to : it.second.raw_config->reference_to) {
        auto& counter = config_namespace.referenced_by_by_document[reference_to][it.second.document];

        spdlog::debug(
          "Increasing referenced_by counter (override_path: '{}', reference_to: '{}', counter: {})",
          it.first,
          reference_to,
          counter.v
        );

        counter.v += 1;
      }

      spdlog::debug(
        "Updating raw config (override_path: '{}', old_id: {}, new_id: {})",
        it.first,
        it.second.raw_config->id,
        config_namespace.next_raw_config_id
      );

      it.second.raw_config->id = config_namespace.next_raw_config_id++;

      override_metadata.raw_config_by_version.emplace(
        config_namespace.current_version,
        std::move(it.second.raw_config)
      );
    }

    for (size_t i = 0; i < override_metadata.watchers.size();) {
      if (auto watcher = override_metadata.watchers[i].lock()) {
        watchers_to_trigger.insert(watcher);
        ++i;
      } else {
        jmutils::swap_delete(override_metadata.watchers, i);
      }
    }
  }
}

} /* scheduler */
} /* mhconfig */
