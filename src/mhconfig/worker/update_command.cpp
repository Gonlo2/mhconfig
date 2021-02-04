#include "mhconfig/worker/update_command.h"

namespace mhconfig
{
namespace worker
{

std::string UpdateCommand::name() const {
  return "UPDATE";
}

bool UpdateCommand::force_take_metric() const {
  return true;
}

bool UpdateCommand::execute(
  context_t* ctx
) {
  bool ok = true;

  cn_->mutex.Lock();
  while (!cn_->update_requests_waiting.empty()) {
    std::shared_ptr<api::request::UpdateRequest> request = nullptr;
    std::swap(request, cn_->update_requests_waiting.front());
    cn_->update_requests_waiting.pop_front();
    request->set_namespace_id(cn_->id);
    if ((cn_->status != ConfigNamespaceStatus::DELETED) && (cn_->current_version == 0xffff)) {
      cn_->mutex.Unlock();
      remove_cn(ctx, cn_->root_path, cn_->id);
      cn_->mutex.Lock();
      delete_cn_locked(cn_);
    }
    bool is_deleted = cn_->status == ConfigNamespaceStatus::DELETED;
    cn_->mutex.Unlock();
    if (is_deleted || !process(ctx, request.get())) {
      request->set_status(api::request::UpdateRequest::Status::ERROR);
      request->commit();
      ok = false;
    }
    cn_->mutex.Lock();
  }
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watch_requests_waiting;
  if (cn_->status == ConfigNamespaceStatus::OK_UPDATING) {
    cn_->status = ConfigNamespaceStatus::OK;
    std::swap(watch_requests_waiting, cn_->watch_requests_waiting);
  }
  cn_->mutex.Unlock();

  for (size_t i = 0, l = watch_requests_waiting.size(); i < l; ++i) {
    process_watch_request(
      decltype(cn_)(cn_),
      std::move(watch_requests_waiting[i]),
      ctx
    );
  }

  return ok;
}

bool UpdateCommand::process(
  context_t* ctx,
  api::request::UpdateRequest* request
) {
  files_to_update_t files_to_update;
  if (!index_request_files(request, files_to_update)) {
    return false;
  }

  files_to_update_t files_to_remove;
  if (request->reload()) {
    spdlog::debug("Obtaining the missing documents to remove");
    files_to_remove = obtain_missing_files(files_to_update);
  }

  spdlog::debug("Filtering the existing documents");
  filter_existing_documents(files_to_update);

  for (auto& it : files_to_remove) {
    files_to_update[it.first].merge(it.second);
  }

  bool ok = true;

  if (files_to_update.empty()) {
    spdlog::debug("Without items to update");
  } else {
    spdlog::debug(
      "Some config files changes, updating the namespace with ID {}",
      cn_->current_version,
      cn_->id
    );

    spdlog::debug("Updating the reference counter");
    ok = update_reference_counters(files_to_update);

    absl::flat_hash_map<
      std::string,
      absl::flat_hash_map<Labels, AffectedDocumentStatus>
    > dep_by_doc;

    if (ok) {
      absl::flat_hash_map<
        Labels,
        absl::flat_hash_map<std::string, AffectedDocumentStatus>
      > dep_by_label;

      spdlog::debug("Updating the ids of the affected documents");
      // If some config will be removed and that document use a reference to another
      // modified document, we need to create a empty version to invalidate the cache
      for (const auto& it: files_to_update) {
        for (const auto& it2: it.second) {
          auto status = (it2.second.status == LoadRawConfigStatus::FILE_DONT_EXISTS)
            ? AffectedDocumentStatus::TO_REMOVE
            : AffectedDocumentStatus::TO_ADD;

          dep_by_label[it2.first][it.first] = status;
        }
      }

      dep_by_doc = get_dep_by_doc(cn_.get(), dep_by_label);

      ok = touch_affected_documents(cn_.get(), cn_->current_version, dep_by_doc, false);
    }

    if (ok) {
      spdlog::debug("Inserting the elements of the updated documents");
      ok = update_documents(files_to_update);
    }

    if (ok) {
      spdlog::debug(
        "Updating the version of the config namespace '{}' to {}",
        cn_->root_path,
        cn_->current_version+1
      );

      cn_->mutex.Lock();
      cn_->current_version += 1;
      cn_->stored_versions.back().second = jmutils::monotonic_now_sec();
      cn_->stored_versions.emplace_back(cn_->current_version, 0);
      cn_->mutex.Unlock();

      spdlog::debug("Checking the watchers to trigger");
      trigger_watchers(ctx, dep_by_doc);
    } else {
      remove_cn(ctx, cn_->root_path, cn_->id);
      delete_cn(cn_);
    }
  }

  if (ok) {
    request->set_status(api::request::UpdateRequest::Status::OK);
    request->set_version(cn_->current_version);
    request->commit();
  }

  return ok;
}

bool UpdateCommand::index_request_files(
  const api::request::UpdateRequest* request,
  files_to_update_t& files_to_update
) {
  if (request->reload()) {
    return index_files(
      cn_->pool.get(),
      request->root_path(),
      [&files_to_update](auto&& labels, auto&& result) {
        if (result.status != LoadRawConfigStatus::OK) {
          return false;
        }
        files_to_update[result.document][labels] = std::move(result);
        return true;
      }
    );
  }

  std::filesystem::path root_path(request->root_path());
  for (const auto& x : request->relative_paths()) {
    std::filesystem::path relative_file_path(x);
    auto path = root_path / relative_file_path;
    auto result = index_file(cn_->pool.get(), root_path, path);
    if ((result.status == LoadRawConfigStatus::OK) || (result.status == LoadRawConfigStatus::FILE_DONT_EXISTS)) {
      auto labels = get_path_labels(relative_file_path.parent_path());
      if (!labels) {
        return false;
      }
      files_to_update[result.document][*labels] = std::move(result);
    } else {
      return false;
    }
  }

  return true;
}

UpdateCommand::files_to_update_t UpdateCommand::obtain_missing_files(
  files_to_update_t& files_to_update
) {
  std::vector<std::string> documents_to_remove;

  files_to_update_t result;
  cn_->mutex.ReaderLock();
  for (auto& it: cn_->document_versions_by_name) {
    it.second->mutex.ReaderLock();
    if (!it.second->document_by_version.empty()) {
      auto document = std::prev(it.second->document_by_version.end())->second.get();
      auto& document_changes = files_to_update[document->name];

      document->mutex.ReaderLock();
      document->lbl_set.for_each(
        [&document_changes, &doc_name=it.first, &result](
          const auto& labels,
          auto* override_
        ) -> bool {
          if (
            (get_last_raw_config_locked(override_) != nullptr)
            && (document_changes.count(labels) == 0)
          ) {
            spdlog::debug(
              "Adding the labels {} of the document '{}' to remove",
              labels,
              doc_name
            );

            load_raw_config_result_t item;
            item.status = LoadRawConfigStatus::FILE_DONT_EXISTS;
            item.document = doc_name;
            item.raw_config = nullptr;

            result[doc_name][labels] = std::move(item);
          }

          return true;
        }
      );
      document->mutex.ReaderUnlock();

      if (document_changes.empty()) {
        documents_to_remove.push_back(it.first);
      }
    }
    it.second->mutex.ReaderUnlock();
  }
  cn_->mutex.ReaderUnlock();

  for (size_t i = 0, l = documents_to_remove.size(); i < l; ++i) {
    files_to_update.erase(documents_to_remove[i]);
  }

  return result;
}

void UpdateCommand::filter_existing_documents(
  files_to_update_t& files_to_update
) {
  std::vector<std::string> documents_to_remove;
  std::vector<Labels> labels_to_remove;

  cn_->mutex.ReaderLock();
  for (auto& document_it: files_to_update) {
    auto document = get_document_locked(
      cn_.get(),
      document_it.first,
      cn_->current_version
    );
    if (document == nullptr) continue;
    cn_->mutex.ReaderUnlock();

    labels_to_remove.clear();

    document->mutex.ReaderLock();
    for (const auto& it : document_it.second) {
      if (it.second.status == LoadRawConfigStatus::OK) {
        auto rc = get_raw_config_locked(document.get(), it.first, cn_->current_version);
        if (
            (rc != nullptr)
            && rc->has_content
            && (rc->checksum == it.second.raw_config->checksum)
        ) {
          labels_to_remove.push_back(it.first);
        }
      }
    }
    document->mutex.ReaderUnlock();

    for (size_t i = 0, l = labels_to_remove.size(); i < l; ++i) {
      spdlog::debug(
        "Filtering the existing raw config (document: '{}', labels: {})",
        document_it.first,
        labels_to_remove[i]
      );
      document_it.second.erase(labels_to_remove[i]);
    }

    if (document_it.second.empty()) {
      documents_to_remove.push_back(document_it.first);
    }

    cn_->mutex.ReaderLock();
  }
  cn_->mutex.ReaderUnlock();

  for (size_t i = 0, l = documents_to_remove.size(); i < l; ++i) {
    files_to_update.erase(documents_to_remove[i]);
  }
}

bool UpdateCommand::update_reference_counters(
  files_to_update_t& files_to_update
) {
  absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<std::string, ::jmutils::zero_value_t<int32_t>>
  > delta_tmp;

  cn_->mutex.ReaderLock();
  for (auto& document_it: files_to_update) {
    auto document = get_document_locked(
      cn_.get(),
      document_it.first,
      cn_->current_version
    );
    if (document == nullptr) continue;
    cn_->mutex.ReaderUnlock();

    document->mutex.ReaderLock();
    for (const auto& it : document_it.second) {
      if (it.second.status == LoadRawConfigStatus::OK) {
        for (const auto& reference_to : it.second.raw_config->reference_to) {
          ++delta_tmp[reference_to][it.second.document].v;
        }
      }

      auto rc = get_raw_config_locked(document.get(), it.first, cn_->current_version);
      if ((rc != nullptr) && rc->has_content) {
        for (const auto& reference_to : rc->reference_to) {
          --delta_tmp[reference_to][it.second.document].v;
        }
      }
    }
    document->mutex.ReaderUnlock();

    cn_->mutex.ReaderLock();
  }
  cn_->mutex.ReaderUnlock();

  absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<std::string, int32_t>
  > delta;

  delta.reserve(delta_tmp.size());
  for (auto& document_it: delta_tmp) {
    for (const auto& it: document_it.second) {
      if (it.second.v != 0) {
        delta[document_it.first][it.first] = it.second.v;
      }
    }
  }

  if (!delta.empty()) {
    cn_->mutex.Lock();
    for (auto& document_it: delta) {
      auto dv = get_or_build_document_versions_locked(cn_.get(), document_it.first);
      for (const auto& it : document_it.second) {
        auto& counter = dv->referenced_by[it.first];
        spdlog::debug(
          "Updating referenced_by counter (document: '{}', reference_to: '{}', current: {}, new: {})",
          document_it.first,
          it.first,
          counter.v,
          counter.v + it.second
        );

        counter.v += it.second;
      }
    }
    cn_->mutex.Unlock();
  }

  return true;
}

bool UpdateCommand::update_documents(
  files_to_update_t& files_to_update
) {
  for (auto& document_it: files_to_update) {
    auto document = try_obtain_non_full_document(
      cn_.get(),
      document_it.first,
      cn_->current_version+1,
      document_it.second.size()
    );
    if (document == nullptr) return false;

    for (const auto& it : document_it.second) {
      auto* override_ = document->lbl_set.get_or_build(it.first);

      if (it.second.status == LoadRawConfigStatus::FILE_DONT_EXISTS) {
        spdlog::debug(
          "Removing raw config if possible (document: '{}', labels: {})",
          document_it.first,
          it.first
        );

        override_->raw_config_by_version.emplace(cn_->current_version+1, nullptr);
      } else {
        spdlog::debug(
          "Updating raw config (document: '{}', labels: {}, id: {})",
          document_it.first,
          it.first,
          document->next_raw_config_id
        );

        it.second.raw_config->id = document->next_raw_config_id++;
        if (it.second.raw_config->logger != nullptr) {
          it.second.raw_config->logger->change_all(
            document->id,
            it.second.raw_config->id
          );
        }
        it.second.raw_config->value.walk_mut(
          [document_id=document->id, raw_config_id=it.second.raw_config->id](auto* e) {
            e->set_document_id(document_id);
            e->set_raw_config_id(raw_config_id);
          }
        );
        it.second.raw_config->value.freeze();

        document->raw_config_by_id[it.second.raw_config->id] = it.second.raw_config;
        override_->raw_config_by_version[cn_->current_version+1] = std::move(it.second.raw_config);
      }
    }
    document->mutex.Unlock();
  }

  return true;
}

void UpdateCommand::trigger_watchers(
  context_t* ctx,
  const absl::flat_hash_map<std::string, absl::flat_hash_map<Labels, AffectedDocumentStatus>>& dep_by_doc
) {
  std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watchers;

  cn_->mutex.ReaderLock();
  for (const auto& it : dep_by_doc) {
    if (
      auto search = cn_->document_versions_by_name.find(it.first);
      search != cn_->document_versions_by_name.end()
    ) {
      search->second->watchers.for_each(
        [&deps=it.second, &watchers](auto&& watcher) {
          for (const auto& it : deps) {
            if (watcher->labels().contains(it.first)) {
              watchers.push_back(std::move(watcher));
              break;
            }
          }
        }
      );
    }
  }
  cn_->mutex.ReaderUnlock();

  for (size_t i = 0, l = watchers.size(); i < l; ++i) {
    spdlog::debug(
      "The document '{}' changed and it has a watcher",
      watchers[i]->document()
    );

    auto msg = watchers[i]->make_output_message();

    process_get_config_task(
      decltype(cn_)(cn_),
      std::make_shared<ApiGetConfigTask>(
        std::make_shared<api::stream::WatchGetRequest>(
          cn_->current_version,
          std::move(watchers[i]),
          std::move(msg)
        )
      ),
      ctx
    );
  }
}

} /* worker */
} /* mhconfig */
