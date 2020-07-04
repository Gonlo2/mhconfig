#include "mhconfig/scheduler/update_documents_command.h"

namespace mhconfig
{
namespace scheduler
{

UpdateDocumentsCommand::UpdateDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request,
  std::vector<load_raw_config_result_t>&& items
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

  std::vector<std::pair<std::string, std::string>> to_remove;
  if (update_request_->reload()) {
    spdlog::debug("Obtaining the existing documents to remove");
    fill_config_to_remove(config_namespace, to_remove);
  }

  spdlog::debug("Filtering the existing documents");
  filter_existing_documents(config_namespace);

  size_t l1 = items_.size();
  size_t l2 = to_remove.size();
  items_.resize(l1 + l2);
  for (size_t i = 0; i < l2; ++i) {
    spdlog::debug(
      "Adding a document to remove (document: '{}', override: '{}')",
      to_remove[i].first,
      to_remove[i].second
    );

    items_[l1+i].status = LoadRawConfigStatus::FILE_DONT_EXISTS;
    items_[l1+i].document = to_remove[i].first;
    items_[l1+i].override_ = to_remove[i].second;
    items_[l1+i].raw_config = nullptr;
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
      .first = jmutils::time::monotonic_now_sec();

    config_namespace.stored_versions_by_deprecation_timestamp
      .emplace_back(0, config_namespace.current_version);


    spdlog::debug("Decreasing the referenced_by counter of the previous raw config");
    decrease_references(config_namespace);

    absl::flat_hash_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers_to_trigger;

    spdlog::debug("Updating the ids of the affected documents");
    increment_version_of_the_affected_documents(config_namespace, watchers_to_trigger);

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
      (config_namespace.next_raw_config_id >= 0xff000000)
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

void UpdateDocumentsCommand::fill_config_to_remove(
  config_namespace_t& config_namespace,
  std::vector<std::pair<std::string, std::string>>& result
) {
  absl::flat_hash_map<std::string, std::set<std::string>> new_configs;
  for (size_t i = 0; i < items_.size(); ++i) {
    new_configs[items_[i].document].insert(items_[i].override_);
  }

  for (const auto document_metadata_it: config_namespace.document_metadata_by_document) {
    auto search = new_configs.find(document_metadata_it.first);
    if (search == new_configs.end()) {
      for (const auto override_it : document_metadata_it.second.override_by_key) {
        if (has_last_version(override_it.second)) {
          result.emplace_back(document_metadata_it.first, override_it.first);
        }
      }
    } else {
      for (const auto override_it : document_metadata_it.second.override_by_key) {
        if ((search->second.count(override_it.first) == 0) && has_last_version(override_it.second)) {
          result.emplace_back(document_metadata_it.first, override_it.first);
        }
      }
    }
  }
}

void UpdateDocumentsCommand::filter_existing_documents(
  config_namespace_t& config_namespace
) {
  for (size_t i = 0; i < items_.size();) {
    bool move_to_next = true;

    if (items_[i].status == LoadRawConfigStatus::OK) {
      auto document_metadata_search = config_namespace.document_metadata_by_document
        .find(items_[i].document);

      if (document_metadata_search != config_namespace.document_metadata_by_document.end()) {
        with_raw_config(
          document_metadata_search->second,
          items_[i].override_,
          0,
          [&](const auto& raw_config) {
            if (raw_config->crc32 == items_[i].raw_config->crc32) {
              spdlog::debug(
                "Filtering the existing raw config (document: '{}', override: '{}', crc32: {:X})",
                items_[i].document,
                items_[i].override_,
                raw_config->crc32
              );
              jmutils::swap_delete(items_, i);
              move_to_next = false;
            }
          }
        );
      }
    }

    if (move_to_next) ++i;
  }
}

void UpdateDocumentsCommand::decrease_references(
  config_namespace_t& config_namespace
) {
  for (auto& item : items_) {
    auto document_metadata_search = config_namespace.document_metadata_by_document
      .find(item.document);

    if (document_metadata_search != config_namespace.document_metadata_by_document.end()) {
      with_raw_config(
        document_metadata_search->second,
        item.override_,
        0,
        [&](const auto& raw_config) {
          for (const auto& reference_to : raw_config->reference_to) {
            auto& referenced_by = config_namespace
              .document_metadata_by_document[reference_to]
              .referenced_by;

            spdlog::debug(
              "Decreasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
              item.document,
              reference_to,
              referenced_by[item.document].v,
              item.override_
            );

            if (referenced_by[item.document].v-- <= 1) {
              referenced_by.erase(item.document);
            }
          }
        }
      );
    }
  }
}

void UpdateDocumentsCommand::increment_version_of_the_affected_documents(
  config_namespace_t& config_namespace,
  absl::flat_hash_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>>& watchers_to_trigger
) {
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>> updated_documents_by_override;
  for (auto& item : items_) {
    updated_documents_by_override[item.override_].insert(item.document);
  }

  for (auto& updated_documents_it : updated_documents_by_override) {
    absl::flat_hash_set<std::string> affected_documents(
      updated_documents_it.second.begin(),
      updated_documents_it.second.end()
    );

    get_affected_documents(config_namespace, affected_documents);

    for (const auto& document : updated_documents_it.second) {
      affected_documents.erase(document);
    }

    for (const auto& document : affected_documents) {
      auto& document_metadata = config_namespace
        .document_metadata_by_document[document];

      with_raw_config(
        document_metadata,
        updated_documents_it.first,
        0,
        [&](const auto& raw_config) {
          spdlog::debug(
            "Updating affected raw config id (document: '{}', override: '{}', old_id: {}, new_id: {})",
            document,
            updated_documents_it.first,
            raw_config->id,
            config_namespace.next_raw_config_id
          );

          auto new_raw_config = raw_config->clone(
            config_namespace.next_raw_config_id++
          );

          document_metadata.override_by_key[updated_documents_it.first]
            .raw_config_by_version[config_namespace.current_version] = std::move(new_raw_config);
        }
      );

      auto override_search = document_metadata.override_by_key
        .find(updated_documents_it.first);

      if (override_search != document_metadata.override_by_key.end()) {
        auto& watchers = override_search->second.watchers;
        for (size_t i = 0; i < watchers.size();) {
          if (auto watcher = watchers[i].lock()) {
            watchers_to_trigger.insert(watcher);
            ++i;
          } else {
            jmutils::swap_delete(watchers, i);
          }
        }
      }
    }
  }
}

void UpdateDocumentsCommand::insert_updated_documents(
  config_namespace_t& config_namespace,
  absl::flat_hash_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>>& watchers_to_trigger
) {
  for (auto& item : items_) {
    auto document_metadata_search = config_namespace.document_metadata_by_document
      .find(item.document);

    if (item.raw_config == nullptr) {
      spdlog::debug(
        "Removing a raw config (document: '{}', override: '{}')",
        item.document,
        item.override_
      );
      if (document_metadata_search != config_namespace.document_metadata_by_document.end()) {
        document_metadata_search->second.override_by_key[item.override_]
          .raw_config_by_version[config_namespace.current_version] = nullptr;
      }
    } else {
      for (const auto& reference_to : item.raw_config->reference_to) {
        auto& referenced_document_metadata = config_namespace.document_metadata_by_document[reference_to];

        spdlog::debug(
          "Increasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
          item.document,
          reference_to,
          referenced_document_metadata.referenced_by[item.document].v,
          item.override_
        );

        referenced_document_metadata.referenced_by[item.document].v += 1;
      }

      auto& document_metadata = config_namespace.document_metadata_by_document[item.document];
      spdlog::debug(
        "Updating a raw config (document: '{}', override: '{}', new_id: {})",
        item.document,
        item.override_,
        config_namespace.next_raw_config_id
      );

      item.raw_config->id = config_namespace.next_raw_config_id++;
      document_metadata.override_by_key[item.override_]
        .raw_config_by_version[config_namespace.current_version] = item.raw_config;
    }

    if (document_metadata_search != config_namespace.document_metadata_by_document.end()) {
      auto override_search = document_metadata_search->second
        .override_by_key
        .find(item.override_);

      if (override_search != document_metadata_search->second.override_by_key.end()) {
        auto& watchers = override_search->second.watchers;
        for (size_t i = 0; i < watchers.size();) {
          if (auto watcher = watchers[i].lock()) {
            watchers_to_trigger.insert(watcher);
            ++i;
          } else {
            jmutils::swap_delete(watchers, i);
          }
        }
      }
    }
  }
}

void UpdateDocumentsCommand::get_affected_documents(
  const config_namespace_t& config_namespace,
  absl::flat_hash_set<std::string>& affected_documents
) {
  std::vector<std::string> to_check(
    affected_documents.begin(),
    affected_documents.end()
  );

  while (!to_check.empty()) {
    std::string doc = to_check.back();
    to_check.pop_back();

    auto search = config_namespace.document_metadata_by_document.find(doc);
    if (search != config_namespace.document_metadata_by_document.end()) {
      for (const auto& referenced_document_it : search->second.referenced_by) {
        auto inserted = affected_documents.insert(referenced_document_it.first);
        if (inserted.second) {
          to_check.push_back(referenced_document_it.first);
        }
      }
    }
  }
}

} /* scheduler */
} /* mhconfig */
