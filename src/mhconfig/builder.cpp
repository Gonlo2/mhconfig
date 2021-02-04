#include "mhconfig/builder.h"

namespace mhconfig
{

// Setup logic

bool init_config_namespace(
  config_namespace_t* cn
) {
  cn->id = std::uniform_int_distribution<uint64_t>{0, 0xffffffffffffffff}(jmutils::prng_engine());
  cn->last_access_timestamp = 0;

  absl::flat_hash_map<
    Labels,
    absl::flat_hash_map<std::string, AffectedDocumentStatus>
  > updated_documents_by_labels;

  bool ok = index_files(
    cn->pool.get(),
    cn->root_path,
    [cn, &updated_documents_by_labels](auto&& labels, auto&& result) {
      if (result.status != LoadRawConfigStatus::OK) {
        return false;
      }

      cn->mutex.Lock();
      for (const auto& reference_to : result.raw_config->reference_to) {
        get_or_build_document_versions_locked(cn, reference_to)->referenced_by[result.document].v += 1;
      }
      cn->mutex.Unlock();

      auto document = try_obtain_non_full_document(
        cn,
        result.document,
        1
      );
      if (document == nullptr) return false;

      result.raw_config->id = document->next_raw_config_id++;
      if (result.raw_config->logger != nullptr) {
        result.raw_config->logger->change_all(
          document->id,
          result.raw_config->id
        );
      }
      result.raw_config->value.walk_mut(
        [document_id=document->id, raw_config_id=result.raw_config->id](auto* e) {
          e->set_document_id(document_id);
          e->set_raw_config_id(raw_config_id);
        }
      );
      result.raw_config->value.freeze();

      document->raw_config_by_id[result.raw_config->id] = result.raw_config;
      auto override_ = document->lbl_set.get_or_build(labels);
      override_->raw_config_by_version[1] = std::move(result.raw_config);
      document->mutex.Unlock();

      updated_documents_by_labels[labels][result.document] = AffectedDocumentStatus::TO_ADD;

      return true;
    }
  );

  if (!ok) return false;

  spdlog::debug("Setting the ids of the nonexistent affected documents");

  auto dep_by_doc = get_dep_by_doc(cn, updated_documents_by_labels);

  if (!touch_affected_documents(cn, 1, dep_by_doc, true)) {
    return false;
  }

  cn->last_access_timestamp = jmutils::monotonic_now_sec();
  cn->stored_versions.emplace_back(0, cn->current_version);

  return true;
}

std::optional<std::string> read_file(
  const std::filesystem::path& path
) {
  try {
    spdlog::debug("Loading file '{}'", path.string());
    std::ifstream fin(path.string());
    if (!fin.good()) {
      spdlog::error("Some error take place reading the file '{}'", path.string());
      return std::optional<std::string>();
    }

    std::string data;
    fin.seekg(0, std::ios::end);
    data.reserve(fin.tellg());
    fin.seekg(0, std::ios::beg);
    data.assign(std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>());

    return std::optional<std::string>(data);
  } catch(const std::exception &e) {
    spdlog::error(
      "Error reading the file '{}': {}",
      path.string(),
      e.what()
    );
  } catch(...) {
    spdlog::error(
      "Unknown error reading the file '{}'",
      path.string()
    );
  }

  return std::optional<std::string>();
}

load_raw_config_result_t index_file(
  jmutils::string::Pool* pool,
  const std::filesystem::path& root_path,
  const std::filesystem::path& path
) {
  load_raw_config_result_t result;
  result.status = LoadRawConfigStatus::ERROR;

  auto first_filename_char = path.filename().native()[0];
  if (first_filename_char == '.') {
    result.status = LoadRawConfigStatus::INVALID_FILENAME;
    return result;
  }

  auto stem = path.stem().string();
  auto split_result = split_filename(stem);
  if (!split_result.ok) {
    return result;
  }

  absl::flat_hash_set<std::string> reference_to;

  if (first_filename_char == '_') {
    result.document = split_result.kind;
    result.document += '.';
    result.document += split_result.name;
    result.document += path.extension().string();

    if (split_result.kind == "_bin") {
      load_raw_config(
        root_path,
        path,
        [pool](auto& logger, const std::string& data, auto& result) {
          result.raw_config->value = Element(pool->add(data), Element::Type::BIN);
          logger.trace("Created binary value", result.raw_config->value);
        },
        result
      );
    } else if (split_result.kind == "_text") {
      load_raw_config(
        root_path,
        path,
        [pool](auto& logger, const std::string& data, auto& result) {
          result.raw_config->value = Element(pool->add(data));
          logger.trace("Created text value", result.raw_config->value);
        },
        result
      );
    } else {
      result.status = LoadRawConfigStatus::INVALID_FILENAME;
    }
  } else if (path.extension() == ".yaml") {
    result.document = split_result.name;

    load_raw_config(
      root_path,
      path,
      [pool, &reference_to](auto& logger, const std::string& data, auto& result) {
        YAML::Node node;
        try {
          node = YAML::Load(data);
        } catch(const YAML::Exception &e) {
          Element position;
          position.set_position(e.mark.line, e.mark.column);

          auto msg = fmt::format("Error parsing the YAML: {}", e.msg);
          logger.error(pool->add(msg), position);

          result.raw_config->value = UNDEFINED_ELEMENT;
          return;
        }

        ElementBuilder element_builder(
          logger,
          pool,
          result.document,
          reference_to
        );

        result.raw_config->value = element_builder.make_and_check(node);
      },
      result
    );
  } else {
    result.status = LoadRawConfigStatus::INVALID_FILENAME;
  }

  if (result.raw_config != nullptr) {
    // Add a virtual reference to force invalidate the config
    // if the mhconfig config change
    if (result.document != "mhconfig") {
      reference_to.emplace("mhconfig");
    }

    result.raw_config->reference_to.reserve(reference_to.size());
    for (const auto& x : reference_to) {
      result.raw_config->reference_to.push_back(x);
    }
  }

  return result;
}

std::optional<Labels> get_path_labels(
  const std::filesystem::path& path
) {
  absl::flat_hash_map<std::string, std::string> labels;
  for (auto it = path.begin(); it != path.end(); ++it) {
    std::string v;

    auto k = it->string();
    auto pos = k.find('=');
    if (pos == std::string::npos) {
      if (++it == path.end()) {
        spdlog::error(
          "The path '{}' has the last label '{}' without a value",
          path.string(),
          k
        );
        return std::optional<Labels>();
      }
      v = it->string();
    } else {
      v = k.substr(pos+1, k.size()-pos-1);
      k = k.substr(0, pos);
    }

    auto inserted = labels.emplace(k, std::move(v));
    if (!inserted.second) {
      spdlog::error(
        "The path '{}' has repeated the label '{}'",
        path.string(),
        k
      );
      return std::optional<Labels>();
    }
  }

  std::vector<label_t> result;
  for (auto& it : labels) {
    result.emplace_back(it.first, it.second);
  }
  return std::optional<Labels>(jmutils::container::make_labels(std::move(result)));
}

absl::flat_hash_map<std::string, absl::flat_hash_map<Labels, AffectedDocumentStatus>> get_dep_by_doc(
  config_namespace_t* cn,
  absl::flat_hash_map<Labels, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_labels
) {
  absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<Labels, AffectedDocumentStatus>
  > dep_by_doc;

  for (auto& it : updated_documents_by_labels) {
    fill_affected_documents(cn, it.second);

    for (const auto& it2 : it.second) {
      dep_by_doc[it2.first][it.first] = it2.second;
    }
  }

  return dep_by_doc;
}

bool touch_affected_documents(
  config_namespace_t* cn,
  VersionId version,
  const absl::flat_hash_map<std::string, absl::flat_hash_map<Labels, AffectedDocumentStatus>>& dep_by_doc,
  bool only_nonexistent
) {
  for (const auto& it : dep_by_doc) {
    auto document = try_obtain_non_full_document(
      cn,
      it.first,
      version,
      it.second.size()
    );
    if (document == nullptr) {
      spdlog::debug("Some error happens obtaining a non full document for '{}'", it.first);
      return false;
    }

    for (const auto& it2 : it.second) {
      if (
        (it2.second == AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY)
        || (it2.second == AffectedDocumentStatus::DEPENDENCY)
      ) {
        auto override_ = document->lbl_set.get_or_build(it2.first);
        if (override_->raw_config_by_version.empty() || !only_nonexistent) {
          auto last_version = get_last_raw_config_locked(override_);
          auto new_raw_config = (last_version != nullptr) && (it2.second != AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY)
            ? last_version->clone()
            : std::make_shared<raw_config_t>();

          spdlog::debug(
            "Updating affected raw config (document: '{}', labels: {}, version: {}, old_id: {}, new_id: {})",
            it.first,
            it2.first,
            version,
            last_version == nullptr ? 0 : last_version->id,
            document->next_raw_config_id
          );

          new_raw_config->id = document->next_raw_config_id++;
          if (new_raw_config->logger != nullptr) {
            new_raw_config->logger->change_all(
              document->id,
              new_raw_config->id
            );
          }
          new_raw_config->value.walk_mut(
            [document_id=document->id, raw_config_id=new_raw_config->id](auto* e) {
              e->set_document_id(document_id);
              e->set_raw_config_id(raw_config_id);
            }
          );
          new_raw_config->value.freeze();

          document->raw_config_by_id[new_raw_config->id] = new_raw_config;
          override_->raw_config_by_version[version] = std::move(new_raw_config);
        }
      }
    }
    document->mutex.Unlock();
  }

  return true;
}

void fill_affected_documents(
  config_namespace_t* cn,
  absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
) {
  std::vector<std::string> to_check;
  to_check.reserve(affected_documents.size());
  for (const auto& it : affected_documents) {
    to_check.push_back(it.first);
  }

  cn->mutex.ReaderLock();
  while (!to_check.empty()) {
    if (
      auto search = cn->document_versions_by_name.find(to_check.back());
      search != cn->document_versions_by_name.end()
    ) {
      search->second->mutex.ReaderLock();
      for (const auto& it : search->second->referenced_by) {
        auto inserted = affected_documents.emplace(
          it.first,
          AffectedDocumentStatus::DEPENDENCY
        );
        if (inserted.second) {
          to_check.push_back(it.first);
        } else if (inserted.first->second == AffectedDocumentStatus::TO_REMOVE) {
          inserted.first->second = AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY;
        }
      }
      search->second->mutex.ReaderUnlock();
    }
    to_check.pop_back();
  }
  cn->mutex.ReaderUnlock();
}

// Get logic

bool dummy_payload_alloc(Element& element, void*& payload) {
  return true;
}

void dummy_payload_dealloc(void* payload) {
}

bool mhc_tokens_payload_alloc(Element& element, void*& payload) {
  auto tokens = new auth::Tokens;
  payload = static_cast<void*>(tokens);
  return tokens->init(element);
}

void mhc_tokens_payload_dealloc(void* payload) {
  delete static_cast<auth::Tokens*>(payload);
}

bool mhc_policy_payload_alloc(Element& element, void*& payload) {
  auto policy = new auth::Policy;
  payload = static_cast<void*>(policy);
  return policy->init(element);
}

void mhc_policy_payload_dealloc(void* payload) {
  delete static_cast<auth::Policy*>(payload);
}

std::shared_ptr<config_namespace_t> get_cn(
  context_t* ctx,
  const std::string& root_path
) {
  spdlog::debug("Obtaining the config namespace for the root path '{}'", root_path);

  ctx->mutex.ReaderLock();
  auto result = get_cn_locked(ctx, root_path);
  ctx->mutex.ReaderUnlock();

  return result;
}

std::shared_ptr<config_namespace_t> get_or_build_cn(
  context_t* ctx,
  const std::string& root_path
) {
  spdlog::debug("Obtaining the config namespace for the root path '{}'", root_path);

  ctx->mutex.ReaderLock();
  auto result = get_cn_locked(ctx, root_path);
  ctx->mutex.ReaderUnlock();

  if (result == nullptr) {
    ctx->mutex.Lock();

    auto inserted = ctx->cn_by_root_path.try_emplace(
      root_path,
      std::make_shared<config_namespace_t>()
    );
    result = inserted.first->second;

    result->last_access_timestamp = jmutils::monotonic_now_sec();

    if (root_path == ctx->mhc_root_path) {
      {
        auto& mc_payload_fun = result->mc_payload_fun_by_document["tokens"];
        mc_payload_fun.alloc = &mhc_tokens_payload_alloc;
        mc_payload_fun.dealloc = &mhc_tokens_payload_dealloc;
      }
      {
        auto& mc_payload_fun = result->mc_payload_fun_by_document["policy"];
        mc_payload_fun.alloc = &mhc_policy_payload_alloc;
        mc_payload_fun.dealloc = &mhc_policy_payload_dealloc;
      }
    }

    ctx->mutex.Unlock();
  }

  return result;
}

bool alloc_payload_locked(
  merged_config_t* merged_config
) {
  merged_config->payload = nullptr;
  if (merged_config->payload_fun.alloc(merged_config->value, merged_config->payload)) {
    return true;
  }
  if (merged_config->payload != nullptr) {
    merged_config->payload_fun.dealloc(merged_config->payload);
    merged_config->payload = nullptr;
  }
  return false;
}

std::shared_ptr<document_t> get_document_locked(
  const config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  if (
    auto versions_search = cn->document_versions_by_name.find(name);
    versions_search != cn->document_versions_by_name.end()
  ) {
    if (
      auto search = versions_search->second->document_by_version.upper_bound(version);
      search != versions_search->second->document_by_version.begin()
    ) {
      return std::prev(search)->second;
    }
  }

  return nullptr;
}

std::optional<DocumentId> next_document_id_locked(
  config_namespace_t* cn
) {
  DocumentId document_id = cn->next_document_id;
  do {
    if (!cn->document_by_id.contains(document_id)) {
      cn->next_document_id = document_id+1;
      return std::optional<DocumentId>(document_id);
    }
  } while (++document_id != cn->next_document_id);
  return std::optional<DocumentId>();
}

bool try_insert_document_locked(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  std::shared_ptr<document_t>& document
) {
  if (auto document_id = next_document_id_locked(cn); document_id) {
    document->id = *document_id;
    document->oldest_version = cn->oldest_version;
    document->name = name;

    get_or_build_document_versions_locked(cn, name)->document_by_version[version] = document;
    cn->document_by_id[*document_id] = document;

    return true;
  }

  return false;
}

std::shared_ptr<document_t> try_get_or_build_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  cn->mutex.ReaderLock();
  auto document = get_document_locked(cn, name, version);
  if (document != nullptr) document->mutex.Lock();
  cn->mutex.ReaderUnlock();

  if (document == nullptr) {
    cn->mutex.Lock();
    document = try_get_or_build_document_locked(
      cn,
      name,
      version
    );
    if (document != nullptr) document->mutex.Lock();
    cn->mutex.Unlock();
  }

  return document;
}

std::shared_ptr<document_t> try_migrate_document_locked(
  config_namespace_t* cn,
  document_t* document,
  VersionId version
) {
  auto new_document = std::make_shared<document_t>();

  bool ok = document->lbl_set.for_each(
    [new_document=new_document.get(), version](
      const auto& labels,
      auto* override_
    ) -> bool {
      if (auto last_version = get_last_raw_config_locked(override_)) {
        if (is_document_full_locked(new_document)) {
          return false;
        }

        auto new_raw_config = last_version->clone();
        new_raw_config->id = new_document->next_raw_config_id++;
        new_document->raw_config_by_id[new_raw_config->id] = new_raw_config;
        auto new_override = new_document->lbl_set.get_or_build(labels);
        new_override->raw_config_by_version[version] = std::move(new_raw_config);
      }
      return true;
    }
  );

  if (!ok) {
    document->mutex.Unlock();
    return nullptr;
  }

  cn->mutex.Lock();
  if (try_insert_document_locked(cn, document->name, version, new_document)) {
    new_document->mutex.Lock();
  } else {
    new_document = nullptr;
  }
  document->mutex.Unlock();
  cn->mutex.Unlock();

  if (new_document != nullptr) {
    new_document->lbl_set.for_each(
      [document_id=new_document->id](const auto&, auto* override_) -> bool {
        if (auto raw_config = get_last_raw_config_locked(override_)) {
          if (raw_config->logger != nullptr) {
            // This change the document_id & raw_config_id of the old documents,
            // but since they both have the same data it shouldn't be a problem
            raw_config->logger->change_all(
              document_id,
              raw_config->id
            );
          }
          raw_config->value.walk_mut(
            [document_id, raw_config_id=raw_config->id](auto* e) {
              e->set_document_id(document_id);
              e->set_raw_config_id(raw_config_id);
            }
          );
          raw_config->value.freeze();
        }
        return true;
      }
    );
  }

  return new_document;
}

std::shared_ptr<document_t> try_obtain_non_full_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  size_t required_size
) {
  auto document = try_get_or_build_document(
    cn,
    name,
    version
  );
  if (document == nullptr) return nullptr;

  if (required_size + document->next_raw_config_id >= 0xffff) {
    auto new_document = try_migrate_document_locked(cn, document.get(), version);
    if (new_document == nullptr) return nullptr;
    std::swap(document, new_document);
    if (required_size + document->next_raw_config_id >= 0xffff) {
      document->mutex.Unlock();
      return nullptr;
    }
  }

  return document;
}

std::shared_ptr<merged_config_t> get_or_build_merged_config(
  document_t* document,
  const std::string& overrides_key
) {
  std::shared_ptr<merged_config_t> result;

  document->mutex.ReaderLock();
  if (
    auto search = document->merged_config_by_overrides_key.find(overrides_key);
    search != document->merged_config_by_overrides_key.end()
  ) {
    result = search->second.lock();
  }
  document->mutex.ReaderUnlock();

  if (result == nullptr) {
    document->mutex.Lock();

    if (
      auto search = document->merged_config_by_overrides_key.find(overrides_key);
      search != document->merged_config_by_overrides_key.end()
    ) {
      result = search->second.lock();
    }

    if (result == nullptr) {
      result = std::make_shared<merged_config_t>();
      result->payload_fun = document->mc_payload_fun;
      result->next = result;

      document->merged_config_by_overrides_key[overrides_key] = result;
      std::swap(result->next, document->mc_generation[0].head);
    }

    document->mutex.Unlock();
  }

  return result;
}

std::shared_ptr<merged_config_t> get_merged_config(
  document_t* document,
  const std::string& overrides_key
) {
  std::shared_ptr<merged_config_t> result;
  bool found = false;

  document->mutex.ReaderLock();
  if (
    auto search = document->merged_config_by_overrides_key.find(overrides_key);
    search != document->merged_config_by_overrides_key.end()
  ) {
    result = search->second.lock();
    found = true;
  }
  document->mutex.ReaderUnlock();

  if (found && (result == nullptr)) {
    document->mutex.Lock();
    if (
      auto search = document->merged_config_by_overrides_key.find(overrides_key);
      search != document->merged_config_by_overrides_key.end()
    ) {
      result = search->second.lock();
      if (result == nullptr) {
        document->merged_config_by_overrides_key.erase(search);
      }
    }
    document->mutex.Unlock();
  }

  return result;
}

split_filename_result_t split_filename(
  std::string_view stem
) {
  split_filename_result_t result;
  result.ok = false;

  if (stem.empty()) {
    result.ok = true;
    return result;
  }

  if (stem[0] == '_') {
    auto pos = stem.find('.');
    if (pos == std::string::npos) {
      return result;
    }
    result.kind = std::string_view(stem.data(), pos);
    stem.remove_prefix(pos+1);
  }

  auto pos = stem.rfind('.');
  result.name = pos == std::string::npos
    ? stem
    : std::string_view(stem.data(), pos);

  result.ok = true;
  return result;
}

std::vector<api::source_t> make_sources(
  const api::SourceIds& source_ids,
  config_namespace_t* cn
) {
  absl::flat_hash_map<DocumentId, absl::flat_hash_set<RawConfigId>> rc_by_doc_id;
  for (const auto& it : source_ids) {
    rc_by_doc_id[it.first].insert(it.second);
  }

  std::vector<api::source_t> sources;

  cn->mutex.ReaderLock();
  for (const auto& doc_it : rc_by_doc_id) {
    spdlog::trace("Finding the document with id {}", doc_it.first);
    auto doc_search = cn->document_by_id.find(doc_it.first);
    if (doc_search != cn->document_by_id.end()) {
      auto doc = doc_search->second;
      doc->mutex.ReaderLock();
      for (auto rc_id : doc_it.second) {
        spdlog::trace("Finding the raw config with id {}", rc_id);
        auto rc_search = doc->raw_config_by_id.find(rc_id);
        if (rc_search != doc->raw_config_by_id.end()) {
          api::source_t source{
            .document_id = doc_it.first,
            .raw_config_id = rc_id,
            .checksum = rc_search->second->file_checksum,
            .path = rc_search->second->path
          };
          sources.push_back(std::move(source));
        } else {
          spdlog::warn("Don't found the raw config with id {}", rc_id);
        }
      }
      doc->mutex.ReaderUnlock();
    } else {
      spdlog::warn("Don't found the document with id {}", doc_it.first);
    }
  }
  cn->mutex.ReaderUnlock();

  return sources;
}

CheckMergedConfigResult check_merged_config(
  merged_config_t* merged_config,
  std::shared_ptr<GetConfigTask>& task,
  bool has_exclusive_lock
) {
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->waiting.push_back(task);
      merged_config->status = MergedConfigStatus::BUILDING;
      return CheckMergedConfigResult::BUILD_CONFIG;

    case MergedConfigStatus::BUILDING: // Fallback
    case MergedConfigStatus::OPTIMIZING:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->waiting.push_back(std::move(task));
      return CheckMergedConfigResult::IN_PROGRESS;

    case MergedConfigStatus::NO_OPTIMIZED:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->status = MergedConfigStatus::OPTIMIZING;
      merged_config->waiting.push_back(std::move(task));
      return CheckMergedConfigResult::OPTIMIZE_CONFIG;

    case MergedConfigStatus::OPTIMIZED:
      spdlog::debug(
        "The built document '{}' has been found",
        task->document()
      );
      return CheckMergedConfigResult::OK;

    case MergedConfigStatus::OPTIMIZATION_FAIL:
      spdlog::debug(
        "The built document '{}' has been found although optimization is wrong",
        task->document()
      );
      return CheckMergedConfigResult::OK;
  }

  task->logger().error("Some error take place");
  return CheckMergedConfigResult::ERROR;
}

void delete_cn_locked(
  std::shared_ptr<config_namespace_t>& cn
) {
  spdlog::debug("Deleting the config namespace for the root path '{}'", cn->root_path);

  cn->status = ConfigNamespaceStatus::DELETED;

  for (auto& it : cn->document_versions_by_name) {
    it.second->watchers.consume(
      [](auto&& watcher) {
        watcher->unregister();

        auto output_message = watcher->make_output_message();
        output_message->set_status(api::stream::WatchStatus::REMOVED);
        output_message->send();
      }
    );
  }

  for (size_t i = 0, l = cn->get_config_tasks_waiting.size(); i < l; ++i) {
    cn->get_config_tasks_waiting[i]->logger().error(
      "The config namespace has been deleted"
    );
    cn->get_config_tasks_waiting[i]->on_complete(
      cn,
      0,
      UNDEFINED_ELEMENT,
      UNDEFINED_ELEMENT_CHECKSUM,
      nullptr
    );
  }
  cn->get_config_tasks_waiting.clear();

  for (auto& request: cn->update_requests_waiting) {
    request->set_namespace_id(cn->id);
    request->set_status(api::request::UpdateRequest::Status::ERROR);
    request->commit();
  }
  cn->update_requests_waiting.clear();

  for (size_t i = 0, l = cn->trace_requests_waiting.size(); i < l; ++i) {
    auto om = cn->trace_requests_waiting[i]->make_output_message();
    om->set_status(api::stream::TraceOutputMessage::Status::ERROR);
    om->send(true);
  }
  cn->trace_requests_waiting.clear();
}

void remove_cn_locked(
  context_t* ctx,
  const std::string& root_path,
  uint64_t id
) {
  spdlog::debug(
    "Removing the config namespace with the root path '{}' and id {}",
    root_path,
    id
  );

  auto search = ctx->cn_by_root_path.find(root_path);
  if (search != ctx->cn_by_root_path.end()) {
    if (search->second->id == id) {
      ctx->cn_by_root_path.erase(search);
    }
  }
}

} /* mhconfig */
