#include "mhconfig/worker/build_command.h"

namespace mhconfig
{
namespace worker
{

BuildCommand::BuildCommand(
  std::shared_ptr<config_namespace_t> cn,
  std::shared_ptr<pending_build_t>&& pending_build
)
  : cn_(std::move(cn)),
  pending_build_(std::move(pending_build))
{
}

std::string BuildCommand::name() const {
  return "BUILD";
}

bool BuildCommand::force_take_metric() const {
  return true;
}

bool BuildCommand::execute(
  context_t* ctx
) {
  if (auto r = check_dependencies(); r != CheckDependenciesStatus::OK) {
    auto merged_config = pending_build_->element->merged_config.get();
    merged_config->mutex.Lock();
    if (r == CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG) {
      merged_config->status = MergedConfigStatus::REF_GRAPH_IS_NOT_DAG;
    } else if (r == CheckDependenciesStatus::MISSING_DEPENDENCY) {
      merged_config->status = MergedConfigStatus::INVALID_VERSION;
    } else if (r == CheckDependenciesStatus::INVALID_VERSION) {
      merged_config->status = MergedConfigStatus::INVALID_VERSION;
    }
    merged_config->mutex.Unlock();
  }

  decrease_pending_elements(ctx, pending_build_.get());
  return true;
}

BuildCommand::CheckDependenciesStatus BuildCommand::check_dependencies() {
  absl::flat_hash_set<std::string> dfs_document_names;
  absl::flat_hash_set<std::string> all_document_names;
  all_document_names.insert(pending_build_->element->document->name);

  cn_->mutex.ReaderLock();
  auto cfg_document = get_document_locked(
    cn_.get(),
    "mhconfig",
    pending_build_->version
  );
  cn_->mutex.ReaderUnlock();

  if (cfg_document == nullptr) {
    return CheckDependenciesStatus::INVALID_VERSION;
  }

  auto cfg = get_element(cfg_document.get(), Labels(), pending_build_->version);

  return check_dependencies_rec(
    pending_build_->element.get(),
    dfs_document_names,
    all_document_names,
    cfg,
    true
  );
}

BuildCommand::CheckDependenciesStatus BuildCommand::check_dependencies_rec(
  build_element_t* build_element,
  absl::flat_hash_set<std::string>& dfs_document_names,
  absl::flat_hash_set<std::string>& all_document_names,
  const Element& cfg,
  bool is_root
) {
  std::string overrides_key;
  absl::flat_hash_set<std::string> reference_to;

  bool is_a_valid_version = for_each_document_override(
    cfg,
    build_element->document.get(),
    pending_build_->task->labels(),
    pending_build_->version,
    [&overrides_key, &reference_to, build_element](auto&& raw_config) {
      jmutils::push_uint16(overrides_key, raw_config->id);
      if (raw_config->has_content) {
        for (size_t i = 0, l = raw_config->reference_to.size(); i < l; ++i) {
          reference_to.insert(raw_config->reference_to[i]);
        }
        build_element->raw_configs_to_merge.push_back(std::move(raw_config));
      }
    }
  );
  if (!is_a_valid_version) {
    return CheckDependenciesStatus::INVALID_VERSION;
  }

  build_element->merged_config = get_or_build_merged_config(
    build_element->document.get(),
    overrides_key
  );
  auto merged_config = build_element->merged_config.get();
  merged_config->last_access_timestamp = jmutils::monotonic_now_sec();

  merged_config->mutex.ReaderLock();
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED: // Fallback
    case MergedConfigStatus::BUILDING:
      break;
    case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED: // Fallback
    case MergedConfigStatus::OK_CONFIG_OPTIMIZING: // Fallback
    case MergedConfigStatus::OK_CONFIG_OPTIMIZED:
      build_element->config = merged_config->value;
      merged_config->mutex.ReaderUnlock();
      return CheckDependenciesStatus::OK;
    case MergedConfigStatus::REF_GRAPH_IS_NOT_DAG:
      merged_config->mutex.ReaderUnlock();
      return CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG;
    case MergedConfigStatus::INVALID_VERSION:
      merged_config->mutex.ReaderUnlock();
      return CheckDependenciesStatus::INVALID_VERSION;
  }
  merged_config->mutex.ReaderUnlock();

  merged_config->mutex.Lock();
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED:
      merged_config->status = MergedConfigStatus::BUILDING;
      merged_config->reference_to = reference_to;
      build_element->to_build = true;
      break;
    case MergedConfigStatus::BUILDING:
      pending_build_->num_pending.fetch_add(1, std::memory_order_relaxed);
      merged_config->to_build.push_back(pending_build_);
      // The root merged config has the building status but
      // in reality is like a undefined one
      if (is_root) {
        merged_config->reference_to = reference_to;
        build_element->to_build = true;
        break;
      }
      merged_config->mutex.Unlock();
      return CheckDependenciesStatus::OK;
    case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED: // Fallback
    case MergedConfigStatus::OK_CONFIG_OPTIMIZING: // Fallback
    case MergedConfigStatus::OK_CONFIG_OPTIMIZED:
      build_element->config = merged_config->value;
      merged_config->mutex.Unlock();
      return CheckDependenciesStatus::OK;
    case MergedConfigStatus::REF_GRAPH_IS_NOT_DAG:
      merged_config->mutex.Unlock();
      return CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG;
    case MergedConfigStatus::INVALID_VERSION:
      merged_config->mutex.Unlock();
      return CheckDependenciesStatus::INVALID_VERSION;
  }
  merged_config->mutex.Unlock();

  dfs_document_names.insert(build_element->document->name);
  for (const auto& name: reference_to) {
    if (dfs_document_names.count(name)) {
      spdlog::error("The config dependencies defined with the '!ref' tag has a cycle");
      return CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG;
    }

    if (auto inserted = all_document_names.insert(name); inserted.second) {
      auto child = std::make_unique<build_element_t>();

      child->document = get_document(cn_.get(), name, pending_build_->version);
      if (child->document == nullptr) {
        return CheckDependenciesStatus::MISSING_DEPENDENCY;
      }

      auto r = check_dependencies_rec(
        child.get(),
        dfs_document_names,
        all_document_names,
        cfg,
        false
      );
      build_element->children.push_back(std::move(child));
      if (r != CheckDependenciesStatus::OK) return r;
    }
  }
  dfs_document_names.erase(build_element->document->name);

  return CheckDependenciesStatus::OK;
}

void BuildCommand::decrease_pending_elements(
  context_t* ctx,
  pending_build_t* pending_build
) {
  if (pending_build->num_pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    absl::flat_hash_map<std::string, merged_config_t*> merged_config_by_document_name;
    auto check_dependencies_status = finish_build_elements_rec(
      ctx,
      pending_build->element.get(),
      merged_config_by_document_name
    );
    if (check_dependencies_status == CheckDependenciesStatus::OK) {
      absl::flat_hash_map<std::string, Element> element_by_document_name;
      build(ctx, pending_build->element.get(), element_by_document_name);
    }
  }
}

BuildCommand::CheckDependenciesStatus BuildCommand::finish_build_elements_rec(
  context_t* ctx,
  build_element_t* build_element,
  absl::flat_hash_map<std::string, merged_config_t*>& merged_config_by_document_name
) {
  auto merged_config = build_element->merged_config.get();
  merged_config_by_document_name[build_element->document->name] = merged_config;

  CheckDependenciesStatus result = CheckDependenciesStatus::OK;
  for (size_t i = 0, l = build_element->children.size(); i < l; ++i) {
    auto check_dependencies_status = finish_build_elements_rec(
      ctx,
      build_element->children[i].get(),
      merged_config_by_document_name
    );
    if (result == CheckDependenciesStatus::OK) {
      result = check_dependencies_status;
    }
  }

  if (build_element->to_build) {
    std::vector<std::shared_ptr<pending_build_t>> to_build;
    std::vector<std::shared_ptr<GetConfigTask>> waiting;

    merged_config->mutex.Lock();

    if (result == CheckDependenciesStatus::OK) {
      auto reference_to = merged_config->reference_to;
      for (const auto& name: reference_to) {
        auto& x = merged_config_by_document_name[name]->reference_to;
        merged_config->reference_to.insert(x.cbegin(), x.cend());
      }
    } else {
      if (result == CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG) {
        merged_config->status = MergedConfigStatus::REF_GRAPH_IS_NOT_DAG;
      } else if (result == CheckDependenciesStatus::MISSING_DEPENDENCY) {
        merged_config->status = MergedConfigStatus::INVALID_VERSION;
      } else if (result == CheckDependenciesStatus::INVALID_VERSION) {
        merged_config->status = MergedConfigStatus::INVALID_VERSION;
      }

      std::swap(merged_config->to_build, to_build);
      std::swap(merged_config->waiting, waiting);
    }

    merged_config->mutex.Unlock();

    if (auto r = get_error_status(result); r) {
      for (size_t i = 0, l = waiting.size(); i < l; ++i) {
        waiting[i]->on_complete(
          *r,
          cn_,
          0,
          UNDEFINED_ELEMENT,
          UNDEFINED_ELEMENT_CHECKSUM,
          nullptr
        );
      }
    }

    for (size_t i = 0, l = to_build.size(); i < l; ++i) {
      decrease_pending_elements(ctx, to_build[i].get());
    }
  } else if (result == CheckDependenciesStatus::OK) {
    merged_config->mutex.ReaderLock();

    if (merged_config->status == MergedConfigStatus::REF_GRAPH_IS_NOT_DAG) {
      result = CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG;
    } else if (merged_config->status == MergedConfigStatus::INVALID_VERSION) {
      result = CheckDependenciesStatus::INVALID_VERSION;
    }

    merged_config->mutex.ReaderUnlock();
  }

  return result;
}

std::optional<GetConfigTask::Status> BuildCommand::get_error_status(
  CheckDependenciesStatus status
) {
  switch (status) {
    case CheckDependenciesStatus::OK:
      return std::optional<GetConfigTask::Status>();
    case CheckDependenciesStatus::REF_GRAPH_IS_NOT_DAG:
      return std::optional<GetConfigTask::Status>(
        GetConfigTask::Status::REF_GRAPH_IS_NOT_DAG
      );
    case CheckDependenciesStatus::MISSING_DEPENDENCY: // Fallback
    case CheckDependenciesStatus::INVALID_VERSION:
      return std::optional<GetConfigTask::Status>(
        GetConfigTask::Status::INVALID_VERSION
      );
  }
  return std::optional<GetConfigTask::Status>(
    GetConfigTask::Status::ERROR
  );
}

void BuildCommand::build(
  context_t* ctx,
  build_element_t* build_element,
  absl::flat_hash_map<std::string, Element>& element_by_document_name
) {
  for (size_t i = 0, l = build_element->children.size(); i < l; ++i) {
    build(ctx, build_element->children[i].get(), element_by_document_name);
  }

  if (build_element->to_build) {
    spdlog::debug(
      "Building the document '{}' with id {}",
      build_element->document->name,
      build_element->document->id
    );

    Element config;

    if (!build_element->raw_configs_to_merge.empty()) {
      config = build_element->raw_configs_to_merge.front()->value;
    }

    for (size_t i = 1, l = build_element->raw_configs_to_merge.size(); i < l; ++i) {
      config = override_with(
        config,
        build_element->raw_configs_to_merge[i]->value,
        element_by_document_name
      );
    }

    if (!config.is_undefined()) {
      config = mhconfig::apply_tags(
        cn_->pool.get(),
        config,
        config,
        element_by_document_name
      ).second;
    }

    config.freeze();

    std::vector<std::shared_ptr<pending_build_t>> to_build;
    std::vector<std::shared_ptr<GetConfigTask>> waiting;

    auto merged_config = build_element->merged_config.get();

    merged_config->mutex.Lock();

    merged_config->value = config;

    auto status = GetConfigTask::Status::OK;
    if (!merged_config->waiting.empty()) {
      merged_config->checksum = config.make_checksum();
      status = alloc_payload_locked(merged_config);
    } else {
      merged_config->status = MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED;
    }

    std::swap(merged_config->to_build, to_build);
    std::swap(merged_config->waiting, waiting);
    merged_config->mutex.Unlock();

    for (size_t i = 0, l = waiting.size(); i < l; ++i) {
      waiting[i]->on_complete(
        status,
        cn_,
        pending_build_->version,
        merged_config->value,
        merged_config->checksum,
        merged_config->payload
      );
    }

    for (size_t i = 0, l = to_build.size(); i < l; ++i) {
      decrease_pending_elements(ctx, to_build[i].get());
    }
  }

  element_by_document_name[build_element->document->name] = build_element->merged_config->value;
}

} /* worker */
} /* mhconfig */
