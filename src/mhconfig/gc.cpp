#include "mhconfig/gc.h"

namespace mhconfig
{

void gc_cn(
  context_t* ctx,
  uint64_t timelimit_s
) {
  spdlog::trace(
    "To remove config namespaces older that {} timestamp",
    timelimit_s
  );

  std::vector<std::shared_ptr<config_namespace_t>> to_remove;

  ctx->mutex.ReaderLock();
  for (auto& it : ctx->cn_by_root_path) {
    spdlog::trace(
      "Checking the namespace '{}' with id {} and timestamp {}",
      it.second->root_path,
      it.second->id,
      it.second->last_access_timestamp
    );

    it.second->mutex.ReaderLock();
    bool check = (it.second->status == ConfigNamespaceStatus::OK)
      || (it.second->status == ConfigNamespaceStatus::OK_UPDATING);

    if (check) {
      if (check = (it.second->last_access_timestamp <= timelimit_s)) {
        for (auto& it2 : it.second->document_versions_by_name) {
          if (!it2.second->watchers.empty()) {
            check = false;
            break;
          }
        }
      }
    }
    it.second->mutex.ReaderUnlock();

    if (check) {
      it.second->mutex.Lock();
      bool remove = (it.second->status == ConfigNamespaceStatus::OK)
        || (it.second->status == ConfigNamespaceStatus::OK_UPDATING);

      if (remove) {
        if (remove = (it.second->last_access_timestamp <= timelimit_s)) {
          for (auto& it2 : it.second->document_versions_by_name) {
            if (!it2.second->watchers.empty()) {
              remove = false;
              break;
            }
          }
        }
      }

      if (remove) {
        spdlog::trace(
          "Removing the namespace '{}' with id {}",
          it.second->root_path,
          it.second->id
        );

        delete_cn_locked(it.second.get());
        to_remove.push_back(it.second);
      }
      it.second->mutex.Unlock();
    }
  }
  ctx->mutex.ReaderUnlock();

  if (!to_remove.empty()) {
    ctx->mutex.Lock();
    for (size_t i = 0, l = to_remove.size(); i < l; ++i) {
      remove_cn_locked(ctx, to_remove[i]->root_path, to_remove[i]->id);
    }
    ctx->mutex.Unlock();
  }
}

void gc_cn_dead_pointers(
  config_namespace_t* cn
) {
  cn->traces_by_override.remove_expired();
  cn->traces_by_flavor.remove_expired();
  cn->traces_by_document.remove_expired();
  cn->to_trace_always.remove_expired();

  cn->mutex.ReaderLock();
  for (auto& it : cn->document_versions_by_name) {
    it.second->watchers.remove_expired();
    it.second->mutex.ReaderLock();
    for (auto& it2: it.second->document_by_version) {
      size_t size = jmutils::remove_expired_map(
        it2.second->mutex,
        it2.second->merged_config_by_overrides_key
      );
      spdlog::trace(
        "Runned GC of the '{}' document merged configs map (root_path: '{}', size: {})",
        it2.second->name,
        cn->root_path,
        size
      );
    }
    it.second->mutex.ReaderUnlock();
  }
  cn->mutex.ReaderUnlock();
}

void gc_cn_raw_config_versions(
  config_namespace_t* cn,
  uint64_t timelimit_s
) {
  spdlog::trace(
    "To remove raw_config versions older that {} timestamp in '{}'",
    timelimit_s,
    cn->root_path
  );

  VersionId oldest_version;

  if (need_clean_cn_raw_config_versions(cn, timelimit_s, oldest_version)) {
    spdlog::trace(
      "Removing raw_config versions older that {} timestamp and v{} in '{}'",
      timelimit_s,
      oldest_version,
      cn->root_path
    );

    std::vector<std::string> documents_to_remove;
    std::vector<DocumentId> document_ids_to_remove;

    cn->mutex.ReaderLock();
    for (auto& it: cn->document_versions_by_name) {
      std::vector<VersionId> versions_to_remove;

      it.second->mutex.ReaderLock();
      for (auto& it2 : it.second->document_by_version) {
        if (it2.first > oldest_version) break;
        if (gc_document_raw_config_versions(it2.second.get(), oldest_version)) {
          versions_to_remove.push_back(it2.first);
        }
      }
      if (it.second->document_by_version.empty()) {
        documents_to_remove.push_back(it.first);
      }
      it.second->mutex.ReaderUnlock();

      if (!versions_to_remove.empty()) {
        it.second->mutex.Lock();
        for (size_t i = 0, l = versions_to_remove.size(); i < l; ++i) {
          if (
            auto search = it.second->document_by_version.find(versions_to_remove[i]);
            search != it.second->document_by_version.end()
          ) {
            if (gc_document_raw_config_versions(search->second.get(), oldest_version)) {
              document_ids_to_remove.push_back(search->second->id);
              it.second->document_by_version.erase(search);
            }
          }
        }
        if (it.second->document_by_version.empty()) {
          documents_to_remove.push_back(it.first);
        }
        it.second->mutex.Unlock();
      }
    }
    cn->mutex.ReaderUnlock();

    if (!document_ids_to_remove.empty() || !documents_to_remove.empty()) {
      cn->mutex.Lock();

      for (size_t i = 0, l = document_ids_to_remove.size(); i < l; ++i) {
        cn->document_ids_in_use.erase(document_ids_to_remove[i]);
      }

      for (size_t i = 0, l = documents_to_remove.size(); i < l; ++i) {
        if (
          auto search = cn->document_versions_by_name.find(documents_to_remove[i]);
          search != cn->document_versions_by_name.end()
        ) {
          if (search->second->document_by_version.empty()) {
            cn->document_versions_by_name.erase(search);
          }
        }
      }

      cn->mutex.Unlock();
    }
  }
}

bool need_clean_cn_raw_config_versions(
  config_namespace_t* cn,
  uint64_t timelimit_s,
  VersionId& oldest_version
) {
  cn->mutex.ReaderLock();
  bool need_clean = ((cn->status == ConfigNamespaceStatus::OK) || (cn->status == ConfigNamespaceStatus::OK_UPDATING))
    && (cn->stored_versions.size() > 1)
    && (cn->stored_versions.front().second <= timelimit_s);
  cn->mutex.ReaderUnlock();

  if (need_clean) {
    cn->mutex.Lock();

    need_clean = ((cn->status == ConfigNamespaceStatus::OK) || (cn->status == ConfigNamespaceStatus::OK_UPDATING))
      && (cn->stored_versions.size() > 1)
      && (cn->stored_versions.front().second <= timelimit_s);

    if (need_clean) {
      do {
        cn->stored_versions.pop_front();
      } while (
        (cn->stored_versions.size() > 1)
        && (cn->stored_versions.front().second <= timelimit_s)
      );
      oldest_version = cn->oldest_version = cn->stored_versions.front().first;
    }

    cn->mutex.Unlock();
  }

  return need_clean;
}

bool gc_document_raw_config_versions(
  document_t* document,
  VersionId oldest_version
) {
  document->mutex.ReaderLock();
  bool need_clean = document->oldest_version < oldest_version;
  document->mutex.ReaderUnlock();

  if (need_clean) {
    document->mutex.Lock();
    need_clean = document->oldest_version < oldest_version;
    if (need_clean) {
      document->oldest_version = oldest_version;
    }
    document->mutex.Unlock();
  }

  if (!need_clean) {
    return false;
  }

  std::vector<std::string> to_check;

  document->mutex.ReaderLock();
  for (const auto& it : document->override_by_path) {
    auto it2 = it.second.raw_config_by_version.begin();
    size_t size = it.second.raw_config_by_version.size();

    bool clean_versions = (size == 0)
      || ((size == 1) && (it2->first < oldest_version) && (it2->second == nullptr))
      || ((size > 1) && (it2->first < oldest_version));

    if (clean_versions) {
      to_check.push_back(it.first);
    }
  }
  bool empty = document->override_by_path.empty();
  document->mutex.ReaderUnlock();

  if (!to_check.empty()) {
    document->mutex.Lock();
    for (size_t i = 0, l = to_check.size(); i < l; ++i) {
      auto search = document->override_by_path.find(to_check[i]);
      if (search != document->override_by_path.end()) {
        if (!search->second.raw_config_by_version.empty()) {
          auto it2 = search->second.raw_config_by_version.upper_bound(oldest_version);
          if (it2 != search->second.raw_config_by_version.begin()) --it2;
          if (it2->second == nullptr) ++it2;

          spdlog::trace(
            "Removed raw_config versions [{}, {}) (document: '{}', override_path: '{}')",
            search->second.raw_config_by_version.begin()->first,
            (it2 == search->second.raw_config_by_version.end()) ? 0xffff : it2->first,
            document->name,
            to_check[i]
          );

          search->second.raw_config_by_version.erase(
            search->second.raw_config_by_version.begin(),
            it2
          );
        }
        if (search->second.raw_config_by_version.empty()) {
          spdlog::trace(
            "Removed override path '{}' in the document '{}'",
            to_check[i],
            document->name
          );

          document->override_by_path.erase(search);
        }
      }
    }
    empty = document->override_by_path.empty();
    document->mutex.Unlock();
  }

  return empty;
}

void gc_cn_merged_configs(
  config_namespace_t* cn,
  uint8_t generation,
  uint64_t timelimit_s
) {
  spdlog::trace(
    "To remove '{}' config namespace merge configs in the {} generation older that {}",
    cn->root_path,
    (uint32_t) generation,
    timelimit_s
  );

  cn->mutex.ReaderLock();
  for (auto& it : cn->document_versions_by_name) {
    it.second->mutex.ReaderLock();
    for (auto& it2 : it.second->document_by_version) {
      gc_document_merged_configs(it2.second.get(), generation, timelimit_s);
    }
    it.second->mutex.ReaderUnlock();
  }
  cn->mutex.ReaderUnlock();
}

void gc_document_merged_configs(
  document_t* document,
  uint8_t generation,
  uint64_t timelimit_s
) {
  document->mc_generation[generation].gc_mutex.Lock();

  document->mutex.ReaderLock();
  auto mc = document->mc_generation[generation].head;
  document->mutex.ReaderUnlock();

  bool has_next = generation + 1 < NUMBER_OF_MC_GENERATIONS;

  GCMergedConfigResult result;
  result.processed = count_same_gc_merged_config(mc.get(), timelimit_s, has_next);

  if (result.processed < 0) {
    std::shared_ptr<merged_config_t> src;
    document->mutex.Lock();
    std::swap(src, document->mc_generation[generation].head);
    document->mutex.Unlock();

    result = gc_merged_config(std::move(src), timelimit_s, has_next);

    if ((result.last != nullptr) || (result.next_last != nullptr)) {
      document->mutex.Lock();
      if (result.last != nullptr) {
        std::swap(result.last->next, document->mc_generation[generation].head);
      }
      if (result.next_last != nullptr) {
        std::swap(result.next_last->next, document->mc_generation[generation+1].head);
      }
      document->mutex.Unlock();
    }
  }

  document->mc_generation[generation].gc_mutex.Unlock();

  spdlog::trace(
    "Runned GC of the '{}' document merged configs generation {} (timelimit: {}, removed: {}, processed: {})",
    document->name,
    (uint32_t) generation,
    timelimit_s,
    result.removed,
    result.processed
  );
}

int32_t count_same_gc_merged_config(
  merged_config_t* mc,
  uint64_t timelimit_s,
  bool has_next
) {
  int32_t processed = 0;
  while (mc != nullptr) {
    switch (obtain_mc_generation(mc, timelimit_s, has_next)) {
      case MCGeneration::SAME:
        ++processed;
        break;
      case MCGeneration::NEXT: // Fallback
      case MCGeneration::NONE:
        return -1;
    }
    mc = mc->next.get();
  }
  return processed;
}

GCMergedConfigResult gc_merged_config(
  std::shared_ptr<merged_config_t>&& src,
  uint64_t timelimit_s,
  bool has_next
) {
  GCMergedConfigResult result;
  while (src != nullptr) {
    std::shared_ptr<merged_config_t> next;
    std::swap(next, src->next);
    switch (obtain_mc_generation(src.get(), timelimit_s, has_next)) {
      case MCGeneration::SAME:
        if (result.last == nullptr) {
          result.last = src;
        }
        src->next = std::move(result.last->next);
        result.last->next = std::move(src);
        break;
      case MCGeneration::NEXT:
        if (result.next_last == nullptr) {
          result.next_last = src;
        }
        src->next = std::move(result.next_last->next);
        result.next_last->next = std::move(src);
        break;
      case MCGeneration::NONE:
        result.removed += 1;
        break;
    }
    std::swap(src, next);
    result.processed += 1;
  }
  return result;
}

} /* mhconfig */
