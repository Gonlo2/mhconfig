#include "mhconfig/scheduler/command/update_documents_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

UpdateDocumentsCommand::UpdateDocumentsCommand(
  uint64_t namespace_id,
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request,
  std::vector<load_raw_config_result_t> items
)
  : Command(),
  namespace_id_(namespace_id),
  update_request_(update_request),
  items_(items)
{
}

UpdateDocumentsCommand::~UpdateDocumentsCommand() {
}

std::string UpdateDocumentsCommand::name() const {
  return "UPDATE_DOCUMENTS";
}

CommandType UpdateDocumentsCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_ID;
}

uint64_t UpdateDocumentsCommand::namespace_id() const {
  return namespace_id_;
}

NamespaceExecutionResult UpdateDocumentsCommand::execute_on_namespace(
  std::shared_ptr<config_namespace_t> config_namespace,
  Queue<CommandRef>& scheduler_queue,
  Queue<worker::command::CommandRef>& worker_queue
) {
  update_request_->set_namespace_id(config_namespace->id);

  spdlog::debug(
    "Increasing the current version {} of the namespace with ID {}",
    config_namespace->current_version,
    config_namespace->id
  );
  ++(config_namespace->current_version);

  config_namespace->stored_versions_by_deprecation_timestamp
    .back()
    .first = jmutils::time::monotonic_now_sec();

  config_namespace->stored_versions_by_deprecation_timestamp
    .emplace_back(0, config_namespace->current_version);

  spdlog::debug("Decreasing the referenced_by counter of the previous raw config");
  for (auto& item : items_) {
    auto document_metadata_search = config_namespace->document_metadata_by_document
      .find(item.document);

    if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
      auto raw_config = get_raw_config(
        document_metadata_search->second,
        item.override_,
        0
      );

      if (raw_config != nullptr) {
        for (const auto& reference_to : raw_config->reference_to) {
          auto& referenced_by = config_namespace
            ->document_metadata_by_document[reference_to]
            ->referenced_by;

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
    }
  }

  std::unordered_set<std::shared_ptr<::mhconfig::api::stream::WatchInputMessage>> watchers_to_trigger;

  spdlog::debug("Updating the id of the affected documents");
  std::unordered_map<std::string, std::unordered_set<std::string>> updated_documents_by_override;
  for (auto& item : items_) {
    updated_documents_by_override[item.override_].insert(item.document);
  }

  for (auto& updated_documents_it : updated_documents_by_override) {
    std::unordered_set<std::string> affected_documents(
      updated_documents_it.second.begin(),
      updated_documents_it.second.end()
    );

    get_affected_documents(config_namespace, affected_documents);

    for (const auto& document : updated_documents_it.second) {
      affected_documents.erase(document);
    }

    for (const auto document : affected_documents) {
      auto document_metadata = config_namespace
        ->document_metadata_by_document[document];

      auto raw_config = get_raw_config(
        document_metadata,
        updated_documents_it.first,
        0
      );

      if (raw_config != nullptr) {
        spdlog::debug(
          "Updating affected raw config id (document: '{}', override: '{}', old_id: {}, new_id: {})",
          document,
          updated_documents_it.first,
          raw_config->id,
          config_namespace->next_raw_config_id
        );

        auto new_raw_config = std::make_shared<raw_config_t>();
        new_raw_config->id = config_namespace->next_raw_config_id++;
        new_raw_config->value = raw_config->value;
        new_raw_config->reference_to = raw_config->reference_to;

        document_metadata->override_by_key[updated_documents_it.first]
          .raw_config_by_version[config_namespace->current_version] = new_raw_config;
      }


      auto override_search = document_metadata->override_by_key
        .find(updated_documents_it.first);

      if (override_search != document_metadata->override_by_key.end()) {
        auto& watchers = override_search->second.watchers;
        for (size_t i = 0; i < watchers.size();) {
          if (auto watcher = watchers[i].lock()) {
            watchers_to_trigger.insert(watcher);
            ++i;
          } else {
            watchers[i] = watchers.back();
            watchers.pop_back();
            --(config_namespace->num_watchers);
          }
        }
      }

    }
  }

  spdlog::debug("Updating the elements of the updated documents");
  for (auto& item : items_) {
    auto document_metadata_search = config_namespace->document_metadata_by_document
      .find(item.document);

    if (item.raw_config == nullptr) {
      spdlog::debug(
        "Removing a raw config (document: '{}', override: '{}')",
        item.document,
        item.override_
      );
      if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
        document_metadata_search->second->override_by_key[item.override_]
          .raw_config_by_version[config_namespace->current_version] = std::make_shared<raw_config_t>();
      }
    } else {
      for (const auto& reference_to : item.raw_config->reference_to) {
        std::shared_ptr<document_metadata_t> referenced_document_metadata;
        {
          auto search = config_namespace->document_metadata_by_document
            .find(reference_to);
          if (search == config_namespace->document_metadata_by_document.end()) {
            referenced_document_metadata = std::make_shared<document_metadata_t>();
            config_namespace->document_metadata_by_document[reference_to] = referenced_document_metadata;
          } else {
            referenced_document_metadata = search->second;
          }
        }

        spdlog::debug(
          "Increasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
          item.document,
          reference_to,
          referenced_document_metadata->referenced_by[item.document].v,
          item.override_
        );

        referenced_document_metadata->referenced_by[item.document].v += 1;
      }

      std::shared_ptr<document_metadata_t> document_metadata;
      if (document_metadata_search == config_namespace->document_metadata_by_document.end()) {
        document_metadata = std::make_shared<document_metadata_t>();
        config_namespace->document_metadata_by_document[item.document] = document_metadata;
      } else {
        document_metadata = document_metadata_search->second;
      }

      spdlog::debug(
        "Updating a raw config (document: '{}', override: '{}', new_id: {})",
        item.document,
        item.override_,
        config_namespace->next_raw_config_id
      );

      item.raw_config->id = config_namespace->next_raw_config_id++;
      document_metadata->override_by_key[item.override_]
        .raw_config_by_version[config_namespace->current_version] = item.raw_config;
    }

    if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
      auto override_search = document_metadata_search->second
        ->override_by_key
        .find(item.override_);

      if (override_search != document_metadata_search->second->override_by_key.end()) {
        auto& watchers = override_search->second.watchers;
        for (size_t i = 0; i < watchers.size();) {
          if (auto watcher = watchers[i].lock()) {
            watchers_to_trigger.insert(watcher);
            ++i;
          } else {
            watchers[i] = watchers.back();
            watchers.pop_back();
            --(config_namespace->num_watchers);
          }
        }
      }
    }
  }

  for (auto watcher : watchers_to_trigger) {
    spdlog::debug(
      "The document '{}' changed and it has a watcher",
      watcher->document()
    );
    auto output_message = watcher->make_output_message();
    output_message->set_uid(watcher->uid());

    auto api_get_command = std::make_shared<scheduler::command::ApiGetCommand>(
      std::make_shared<::mhconfig::api::stream::WatchGetRequest>(
        watcher,
        output_message
      )
    );
    scheduler_queue.push(api_get_command);
  }

  update_request_->set_status(::mhconfig::api::request::update_request::OK);
  update_request_->set_version(config_namespace->current_version);
  send_api_response(worker_queue);

  //Remove the namespace if the versions numbers are on the limit
  if (
    (config_namespace->next_raw_config_id >= 0xff000000)
    || (config_namespace->current_version >= 0xfffffff0)
  ) {
    spdlog::warn(
      "Softdeleting the namespace '{}' because the internal ids are in the limit",
      config_namespace->root_path
    );
    return NamespaceExecutionResult::SOFTDELETE_NAMESPACE;
  }

  return NamespaceExecutionResult::OK;
}

bool UpdateDocumentsCommand::on_get_namespace_error(
  Queue<worker::command::CommandRef>& worker_queue
) {
  send_api_response(worker_queue);
  return true;
}

void UpdateDocumentsCommand::send_api_response(
  Queue<worker::command::CommandRef>& worker_queue
) {
  auto api_reply_command = std::make_shared<::mhconfig::worker::command::ApiReplyCommand>(
    update_request_
  );
  worker_queue.push(api_reply_command);
}

void UpdateDocumentsCommand::get_affected_documents(
  const std::shared_ptr<config_namespace_t> config_namespace,
  std::unordered_set<std::string>& affected_documents
) {
  std::vector<std::string> to_check(
    affected_documents.begin(),
    affected_documents.end()
  );

  while (!to_check.empty()) {
    std::string doc = to_check.back();
    to_check.pop_back();

    auto search = config_namespace->document_metadata_by_document.find(doc);
    if (search != config_namespace->document_metadata_by_document.end()) {
      for (const auto& referenced_document_it : search->second->referenced_by) {
        auto inserted = affected_documents.insert(referenced_document_it.first);
        if (inserted.second) {
          to_check.push_back(referenced_document_it.first);
        }
      }
    }
  }
}

} /* command */
} /* scheduler */
} /* mhconfig */
