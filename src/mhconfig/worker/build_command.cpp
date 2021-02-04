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
  prepare_pending_build();
  decrease_pending_elements(ctx, pending_build_.get());
  return true;
}

void BuildCommand::prepare_pending_build() {
  std::vector<std::string> dfs_doc_names;
  absl::flat_hash_set<std::string> dfs_doc_names_set;
  absl::flat_hash_set<std::string> all_doc_names_set;

  auto build_element = std::move(pending_build_->elements.front());
  pending_build_->elements.pop_front();

  all_doc_names_set.insert(build_element.document->name);

  cn_->mutex.ReaderLock();
  auto cfg_document = get_document_locked(
    cn_.get(),
    "mhconfig",
    pending_build_->version
  );
  cn_->mutex.ReaderUnlock();

  if (cfg_document != nullptr) {
    auto cfg = get_element(cfg_document.get(), Labels(), pending_build_->version);

    prepare_pending_build_rec(
      build_element,
      dfs_doc_names,
      dfs_doc_names_set,
      all_doc_names_set,
      cfg
    );
  }

  pending_build_->elements.push_back(std::move(build_element));
}

void BuildCommand::prepare_pending_build_rec(
  build_element_t& build_element,
  std::vector<std::string>& dfs_doc_names,
  absl::flat_hash_set<std::string>& dfs_doc_names_set,
  absl::flat_hash_set<std::string>& all_doc_names_set,
  const Element& cfg
) {
  std::string overrides_key;
  absl::flat_hash_set<std::string> reference_to;

  bool is_a_valid_version = for_each_document_override(
    cfg,
    build_element.document.get(),
    pending_build_->task->labels(),
    pending_build_->version,
    [&overrides_key, &reference_to, &build_element](auto&& raw_config) {
      jmutils::push_uint16(overrides_key, raw_config->id);
      if (raw_config->has_content) {
        for (size_t i = 0, l = raw_config->reference_to.size(); i < l; ++i) {
          reference_to.insert(raw_config->reference_to[i]);
        }
        build_element.raw_configs_to_merge.push_back(std::move(raw_config));
      }
    }
  );
  if (!is_a_valid_version) return;

  build_element.merged_config = get_or_build_merged_config(
    build_element.document.get(),
    overrides_key
  );
  auto merged_config = build_element.merged_config.get();
  merged_config->last_access_timestamp = jmutils::monotonic_now_sec();

  merged_config->mutex.ReaderLock();
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED: // Fallback
    case MergedConfigStatus::BUILDING:
      break;
    case MergedConfigStatus::NO_OPTIMIZED: // Fallback
    case MergedConfigStatus::OPTIMIZING: // Fallback
    case MergedConfigStatus::OPTIMIZED: // Fallback
    case MergedConfigStatus::OPTIMIZATION_FAIL:
      build_element.config = merged_config->value;
      merged_config->mutex.ReaderUnlock();
      return;
  }
  merged_config->mutex.ReaderUnlock();

  merged_config->mutex.Lock();
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED:
      merged_config->status = MergedConfigStatus::BUILDING;
      merged_config->reference_to = reference_to;
      build_element.to_build = true;
      break;
    case MergedConfigStatus::BUILDING:
      pending_build_->num_pending.fetch_add(1, std::memory_order_relaxed);
      merged_config->to_build.push_back(pending_build_);
      // The root merged config has the building status but
      // in reality is like a undefined one
      if (dfs_doc_names.empty()) {
        merged_config->reference_to = reference_to;
        build_element.to_build = true;
        break;
      }
      merged_config->mutex.Unlock();
      return;
    case MergedConfigStatus::NO_OPTIMIZED: // Fallback
    case MergedConfigStatus::OPTIMIZING: // Fallback
    case MergedConfigStatus::OPTIMIZED: // Fallback
    case MergedConfigStatus::OPTIMIZATION_FAIL:
      build_element.config = merged_config->value;
      merged_config->mutex.Unlock();
      return;
  }
  merged_config->mutex.Unlock();

  dfs_doc_names_set.insert(build_element.document->name);
  dfs_doc_names.push_back(build_element.document->name);
  for (const auto& name: reference_to) {
    if (dfs_doc_names_set.contains(name)) {
      log_cycle(
        build_element,
        name,
        dfs_doc_names,
        dfs_doc_names_set
      );
      continue;
    }

    if (auto inserted = all_doc_names_set.insert(name); inserted.second) {
      build_element_t child;
      child.document = get_document(cn_.get(), name, pending_build_->version);
      if (child.document != nullptr) {
        prepare_pending_build_rec(
          child,
          dfs_doc_names,
          dfs_doc_names_set,
          all_doc_names_set,
          cfg
        );
        pending_build_->elements.push_back(std::move(child));
      }
    }
  }
  dfs_doc_names_set.erase(build_element.document->name);
  dfs_doc_names.pop_back();
}

void BuildCommand::decrease_pending_elements(
  context_t* ctx,
  pending_build_t* pending_build
) {
  if (pending_build->num_pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    build(ctx, pending_build);
  }
}

void BuildCommand::build(
  context_t* ctx,
  pending_build_t* pending_build
) {
  absl::flat_hash_map<std::string, Element> element_by_document_name;
  absl::flat_hash_map<std::string, merged_config_t*> merged_config_by_document_name;

  for (auto& be: pending_build->elements) {
    if (be.to_build) {
      spdlog::debug(
        "Building the document '{}' with id {}",
        be.document->name,
        be.document->id
      );

      ElementMerger merger(
        cn_->pool.get(),
        element_by_document_name
      );

      absl::flat_hash_set<std::string> mc_reference_to;
      for (const auto& name: be.merged_config->reference_to) {
        auto search = merged_config_by_document_name.find(name);
        if (search != merged_config_by_document_name.end()) {
          merger.logger().inject_back(search->second->logger);

          auto& reference_to = search->second->reference_to;
          mc_reference_to.insert(
            reference_to.cbegin(),
            reference_to.cend()
          );
        }
      }

      for (const auto& it : be.raw_configs_to_merge) {
        merger.add(it->logger, it->value);
      }

      Element config = merger.finish();

      std::vector<std::shared_ptr<pending_build_t>> to_build;
      std::vector<std::shared_ptr<GetConfigTask>> waiting;

      be.merged_config->mutex.Lock();

      be.merged_config->value = config;
      be.merged_config->reference_to.merge(mc_reference_to);

      if (!be.merged_config->waiting.empty()) {
        be.merged_config->checksum = config.make_checksum();
        if (alloc_payload_locked(be.merged_config.get())) {
          be.merged_config->status = MergedConfigStatus::OPTIMIZED;
        } else {
          merger.logger().error("Some error take place allocating the payload");
          be.merged_config->status = MergedConfigStatus::OPTIMIZATION_FAIL;
        }
      } else {
        be.merged_config->status = MergedConfigStatus::NO_OPTIMIZED;
      }

      std::swap(be.merged_config->to_build, to_build);
      std::swap(be.merged_config->waiting, waiting);

      be.merged_config->logger.inject_back(merger.logger());
      be.merged_config->logger.remove_duplicates();

      be.merged_config->mutex.Unlock();

      for (size_t i = 0, l = waiting.size(); i < l; ++i) {
        finish_successfully(
          waiting[i].get(),
          cn_,
          pending_build_->version,
          be.merged_config.get()
        );
      }

      for (size_t i = 0, l = to_build.size(); i < l; ++i) {
        decrease_pending_elements(ctx, to_build[i].get());
      }
    }

    merged_config_by_document_name[be.document->name] = be.merged_config.get();
    element_by_document_name[be.document->name] = be.merged_config->value;
  }
}

void BuildCommand::log_cycle(
  build_element_t& cycle_end_be,
  const std::string& cycle_start_doc_name,
  const std::vector<std::string>& dfs_doc_names,
  const absl::flat_hash_set<std::string>& dfs_doc_names_set
) {
  size_t start_idx = 0;
  for (
    size_t l = dfs_doc_names.size();
    (start_idx < l) && (dfs_doc_names[start_idx] != cycle_start_doc_name);
    ++start_idx
  );
  std::stringstream ss;
  ss << "Some references have formed a cycle: ";
  for (size_t l = dfs_doc_names.size(); start_idx < l; ++start_idx) {
    ss << dfs_doc_names[start_idx] << " / ";
  }
  ss << cycle_start_doc_name << " / ...";
  auto message = cn_->pool->add(ss.str());

  for (const auto& be : pending_build_->elements) {
    if (dfs_doc_names_set.contains(be.document->name)) {
      be.merged_config->mutex.Lock();
      be.merged_config->logger.error(
        jmutils::string::String(message)
      );
      be.merged_config->mutex.Unlock();
    }
  }

  cycle_end_be.merged_config->mutex.Lock();
  cycle_end_be.merged_config->logger.error(std::move(message));
  cycle_end_be.merged_config->mutex.Unlock();
}

} /* worker */
} /* mhconfig */
