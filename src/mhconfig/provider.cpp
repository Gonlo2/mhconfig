#include "mhconfig/provider.h"

namespace mhconfig
{

const std::string& ApiGetConfigTask::root_path() const {
  return request_->root_path();
}

uint32_t ApiGetConfigTask::version() const {
  return request_->version();
}

const Labels& ApiGetConfigTask::labels() const {
  return request_->labels();
}

const std::string& ApiGetConfigTask::document() const {
  return request_->document();
}

void ApiGetConfigTask::on_complete(
  std::shared_ptr<config_namespace_t>& cn,
  VersionId version,
  const Element& element,
  const std::array<uint8_t, 32>& checksum,
  void* payload
) {
  if (cn != nullptr) {
    request_->set_namespace_id(cn->id);
  }
  request_->set_version(version);
  request_->set_checksum(checksum.data(), checksum.size());

  api::SourceIds source_ids = sources_logger_.sources();

  if (request_->with_position()) {
    auto element_sids = request_->set_element_with_position(element);
    source_ids.merge(element_sids);
  } else {
    request_->set_element(element);
  }

  if (!source_ids.empty()) {
    auto sources = make_sources(source_ids, cn.get());
    request_->set_sources(sources);
  }

  request_->commit();
}

Logger& ApiGetConfigTask::logger() {
  return logger_;
}

ReplayLogger::Level ApiGetConfigTask::log_level() const {
  switch (request_->log_level()) {
    case api::LogLevel::ERROR:
      return ReplayLogger::Level::error;
    case api::LogLevel::WARN:
      return ReplayLogger::Level::warn;
    case api::LogLevel::DEBUG:
      return ReplayLogger::Level::debug;
    case api::LogLevel::TRACE:
      return ReplayLogger::Level::trace;
  }
  return ReplayLogger::Level::error;
}

AuthPolicyGetConfigTask::AuthPolicyGetConfigTask(
  VersionId version,
  Labels&& labels,
  std::string&& root_path,
  std::shared_ptr<PolicyCheck>&& pc
) : version_(version),
  labels_(std::move(labels)),
  root_path_(root_path),
  pc_(pc)
{
}

AuthPolicyGetConfigTask::~AuthPolicyGetConfigTask() {
}

const std::string& AuthPolicyGetConfigTask::root_path() const {
  return root_path_;
}

uint32_t AuthPolicyGetConfigTask::version() const {
  return version_;
}

const Labels& AuthPolicyGetConfigTask::labels() const {
  return labels_;
}

const std::string& AuthPolicyGetConfigTask::document() const {
  return DOCUMENT_NAME_POLICY;
}

void AuthPolicyGetConfigTask::on_complete(
  std::shared_ptr<config_namespace_t>& cn,
  VersionId version,
  const Element& element,
  const std::array<uint8_t, 32>& checksum,
  void* payload
) {
  if (logger_.num_error_logs() == 0) {
    auto policy = static_cast<auth::Policy*>(payload);
    pc_->on_check_policy(auth::AuthResult::AUTHENTICATED, policy);
  } else {
    pc_->on_check_policy_error();
  }
}

Logger& AuthPolicyGetConfigTask::logger() {
  return logger_;
}

ReplayLogger::Level AuthPolicyGetConfigTask::log_level() const {
  return ReplayLogger::Level::error;
}

AuthTokenGetConfigTask::AuthTokenGetConfigTask(
  std::string& root_path,
  std::string&& token,
  std::shared_ptr<PolicyCheck>&& pc,
  std::shared_ptr<context_t>& ctx
) : root_path_(root_path),
  token_(token),
  pc_(pc),
  ctx_(ctx)
{
}

AuthTokenGetConfigTask::~AuthTokenGetConfigTask() {
}

const std::string& AuthTokenGetConfigTask::root_path() const {
  return root_path_;
}

uint32_t AuthTokenGetConfigTask::version() const {
  return 0;
}

const Labels& AuthTokenGetConfigTask::labels() const {
  return EMPTY_LABELS;
}

const std::string& AuthTokenGetConfigTask::document() const {
  return DOCUMENT_NAME_TOKENS;
}

void AuthTokenGetConfigTask::on_complete(
  std::shared_ptr<config_namespace_t>& cn,
  VersionId version,
  const Element& element,
  const std::array<uint8_t, 32>& checksum,
  void* payload
) {
  if (logger_.num_error_logs() == 0) {
    auto tokens = static_cast<auth::Tokens*>(payload);
    Labels token_labels;
    auto auth_result = tokens->find(token_, token_labels);
    if (auth_result == auth::AuthResult::AUTHENTICATED) {
      process_get_config_task(
        std::shared_ptr<config_namespace_t>(cn),
        std::make_shared<AuthPolicyGetConfigTask>(
          version,
          std::move(token_labels),
          std::move(root_path_),
          std::move(pc_)
        ),
        ctx_.get()
      );
    } else {
      pc_->on_check_policy(auth_result, nullptr);
    }
  } else {
    pc_->on_check_policy_error();
  }
}

Logger& AuthTokenGetConfigTask::logger() {
  return logger_;
}

ReplayLogger::Level AuthTokenGetConfigTask::log_level() const {
  return ReplayLogger::Level::error;
}

void send_existing_watcher_traces(
  config_namespace_t* cn,
  document_versions_t* dv,
  api::stream::TraceInputMessage* trace,
  absl::flat_hash_set<label_t>& labels
) {
  dv->watchers.for_each(
    [cn, trace, &labels](auto&& watcher) {
      bool trigger = true;

      if (!labels.empty()) {
        size_t matches = 0;
        for (const auto& label : watcher->labels()) {
          matches += labels.count(label);
        }
        trigger = matches == labels.size();
      }

      if (trigger) {
        auto om = make_trace_output_message(
          trace,
          api::stream::TraceOutputMessage::Status::EXISTING_WATCHER,
          cn->id,
          0,
          watcher.get()
        );
        om->commit();
      }
    }
  );
}

bool process_get_config_task(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<GetConfigTask>&& task,
  context_t* ctx
) {
  spdlog::debug("Processing the get config task {}", (void*) task.get());

  VersionId version = task->version();
  std::shared_ptr<document_t> cfg_document;
  std::shared_ptr<document_t> document;

  bool ok = false;
  bool require_exclusive_lock = false;

  cn->mutex.ReaderLock();
  switch (cn->status) {
    case ConfigNamespaceStatus::UNDEFINED: // Fallback
    case ConfigNamespaceStatus::BUILDING:
      require_exclusive_lock = true;
      break;
    case ConfigNamespaceStatus::OK: // Fallback
    case ConfigNamespaceStatus::OK_UPDATING:
      version = get_version(cn.get(), version);
      if (version != 0) {
        cfg_document = get_document_locked(cn.get(), "mhconfig", version);
        document = get_document_locked(cn.get(), task->document(), version);
      }
      ok = true;
      break;
    case ConfigNamespaceStatus::DELETED:
      break;
  }
  cn->mutex.ReaderUnlock();

  if (require_exclusive_lock) {
    cn->mutex.Lock();
    switch (cn->status) {
      case ConfigNamespaceStatus::UNDEFINED: {
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = task->root_path();
        cn->get_config_tasks_waiting.push_back(std::move(task));
        cn->mutex.Unlock();

        spdlog::debug(
          "The config namespace '{}' is undefined, setting it!",
          cn->root_path
        );

        execute_command_in_worker_thread(
          std::make_unique<worker::SetupCommand>(std::move(cn)),
          ctx
        );

        return true;
      }
      case ConfigNamespaceStatus::BUILDING:
        cn->get_config_tasks_waiting.push_back(std::move(task));
        cn->mutex.Unlock();
        spdlog::debug("The config namespace '{}' is building, waiting it!", cn->root_path);
        return true;
      case ConfigNamespaceStatus::OK: // Fallback
      case ConfigNamespaceStatus::OK_UPDATING:
        version = get_version(cn.get(), version);
        if (version != 0) {
          cfg_document = get_document_locked(cn.get(), "mhconfig", version);
          document = get_document_locked(cn.get(), task->document(), version);
        }
        ok = true;
        break;
      case ConfigNamespaceStatus::DELETED:
        break;
    }
    cn->mutex.Unlock();
  }

  if (!ok) {
    task->logger().error("Some error take place with the config namespace");
    finish_with_error(task.get(), cn, version);
    return false;
  }

  if (!is_a_valid_document(version, cfg_document.get(), document.get(), task.get())) {
    finish_with_error(task.get(), cn, version);
    return false;
  }

  auto cfg = get_element(cfg_document.get(), Labels(), version);

  std::string overrides_key;

  bool is_a_valid_version = for_each_document_override(
    cfg,
    document.get(),
    task->labels(),
    version,
    [&overrides_key](auto&& raw_config) {
      jmutils::push_uint16(overrides_key, raw_config->id);
    }
  );
  if (!is_a_valid_version) {
    task->logger().error("The asked version don't exists");
    finish_with_error(task.get(), cn, version);
    return true;
  }

  for_each_trace_to_trigger(
    cn.get(),
    task.get(),
    [version, cn=cn.get(), task=task.get()](auto* trace) {
      auto om = make_trace_output_message(
        trace,
        api::stream::TraceOutputMessage::Status::RETURNED_ELEMENTS,
        cn->id,
        version,
        task
      );
      om->commit();
    }
  );

  spdlog::debug(
    "Searching the merged config of a overrides_key with size {}",
    overrides_key.size()
  );

  auto merged_config = get_or_build_merged_config(document.get(), overrides_key);
  // This will have write-write race conditions but this should not be a problem
  merged_config->last_access_timestamp = jmutils::monotonic_now_sec();

  CheckMergedConfigResult check_merged_config_result;

  merged_config->mutex.ReaderLock();
  check_merged_config_result = check_merged_config(
    merged_config.get(),
    task,
    false
  );
  merged_config->mutex.ReaderUnlock();

  switch (check_merged_config_result) {
    case CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK:
      spdlog::debug("The operation need a exclusive lock");
      break;
    case CheckMergedConfigResult::OK:
      finish_successfully(task.get(), cn, version, merged_config.get());
      return true;
    default:
      finish_with_error(task.get(), cn, version);
      return false;
  }

  merged_config->mutex.Lock();
  check_merged_config_result = check_merged_config(
    merged_config.get(),
    task,
    true
  );
  merged_config->mutex.Unlock();

  switch (check_merged_config_result) {
    case CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK:
      assert(false);
      break;
    case CheckMergedConfigResult::IN_PROGRESS:
      spdlog::debug("The operation is in progress");
      return true;
    case CheckMergedConfigResult::BUILD_CONFIG: {
      spdlog::debug("The operation need build the config");

      auto pending_build = std::make_shared<pending_build_t>();
      pending_build->version = version;
      pending_build->task = std::move(task);
      build_element_t be{
        .document = std::move(document),
        .merged_config = std::move(merged_config)
      };
      pending_build->elements.push_back(std::move(be));

      execute_command_in_worker_thread(
        std::make_unique<worker::BuildCommand>(
          std::move(cn),
          std::move(pending_build)
        ),
        ctx
      );

      return true;
    }
    case CheckMergedConfigResult::OPTIMIZE_CONFIG: {
      spdlog::debug("Optimizing and commiting the message");

      execute_command_in_worker_thread(
        std::make_unique<worker::OptimizeCommand>(
          std::move(cn),
          std::move(merged_config)
        ),
        ctx
      );

      return true;
    }
    case CheckMergedConfigResult::OK:
      finish_successfully(task.get(), cn, version, merged_config.get());
      return true;
    case CheckMergedConfigResult::ERROR:
      finish_with_error(task.get(), cn, version);
      return false;
  }

  task->logger().error("Some error take place");
  finish_with_error(task.get(), cn, version);
  return false;
}

bool is_a_valid_document(
  VersionId version,
  document_t* cfg_document,
  document_t* document,
  GetConfigTask* task
) {
  if (version == 0) {
    task->logger().error("The asked version don't exists");
    return false;
  }

  if (document == nullptr) {
    task->logger().error("The asked document version don't exists");
    return false;
  }

  if (cfg_document == nullptr) {
    task->logger().error("The mhconfig document version don't exists");
    return false;
  }

  return true;
}

bool process_update_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::request::UpdateRequest>&& request,
  context_t* ctx
) {
  spdlog::debug("Processing the update request {}", (void*) request.get());

  cn->mutex.Lock();
  switch (cn->status) {
    case ConfigNamespaceStatus::UNDEFINED: {
      cn->status = ConfigNamespaceStatus::BUILDING;
      cn->root_path = request->root_path();
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();

      spdlog::debug(
        "The config namespace '{}' is undefined, setting it!",
        cn->root_path
      );

      execute_command_in_worker_thread(
        std::make_unique<worker::SetupCommand>(std::move(cn)),
        ctx
      );

      return true;
    }
    case ConfigNamespaceStatus::BUILDING: // Fallback
    case ConfigNamespaceStatus::OK_UPDATING:
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();
      spdlog::debug("The config namespace '{}' isn't ready, waiting it!", cn->root_path);
      return true;
    case ConfigNamespaceStatus::OK: {
      cn->status = ConfigNamespaceStatus::OK_UPDATING;
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();

      spdlog::debug("To update the config namespace '{}'", cn->root_path);

      execute_command_in_worker_thread(
        std::make_unique<worker::UpdateCommand>(std::move(cn)),
        ctx
      );

      return true;
    }
    case ConfigNamespaceStatus::DELETED:
      cn->mutex.Unlock();
      spdlog::debug("The config namespace '{}' has been deleted", cn->root_path);
      request->set_status(api::request::UpdateRequest::Status::ERROR);
      request->commit();
      return true;
  }
  cn->mutex.Unlock();

  spdlog::debug("Some error take place");
  request->set_status(api::request::UpdateRequest::Status::ERROR);
  request->commit();
  return false;
}

bool process_watch_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::stream::WatchInputMessage>&& request,
  context_t* ctx
) {
  spdlog::debug("Processing the watch request {}", (void*) request.get());

  VersionId version = 0;

  bool error = true;
  bool require_exclusive_lock = false;

  cn->mutex.ReaderLock();
  switch (cn->status) {
    case ConfigNamespaceStatus::UNDEFINED: // Fallback
    case ConfigNamespaceStatus::BUILDING: // Fallback
    case ConfigNamespaceStatus::OK_UPDATING:
      require_exclusive_lock = true;
      break;
    case ConfigNamespaceStatus::OK: {
      auto search = cn->document_versions_by_name.find(request->document());
      require_exclusive_lock = error = search == cn->document_versions_by_name.end();
      if (!require_exclusive_lock) {
        version = get_version(cn.get(), 0);
        search->second->watchers.add(request);
      }
      break;
    }
    case ConfigNamespaceStatus::DELETED:
      break;
  }
  cn->mutex.ReaderUnlock();

  if (require_exclusive_lock) {
    cn->mutex.Lock();
    switch (cn->status) {
      case ConfigNamespaceStatus::UNDEFINED: {
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = request->root_path();
        cn->watch_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();

        spdlog::debug(
          "The config namespace '{}' is undefined, setting it!",
          cn->root_path
        );

        execute_command_in_worker_thread(
          std::make_unique<worker::SetupCommand>(std::move(cn)),
          ctx
        );

        return true;
      }
      case ConfigNamespaceStatus::BUILDING:
      case ConfigNamespaceStatus::OK_UPDATING:
        cn->watch_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();
        spdlog::debug("The config namespace '{}' isn't prepared, waiting it!", cn->root_path);
        return true;
      case ConfigNamespaceStatus::OK: {
        auto dv = get_or_build_document_versions_locked(cn.get(), request->document());
        version = get_version(cn.get(), 0);
        dv->watchers.add(request);
        error = false;
        break;
      }
      case ConfigNamespaceStatus::DELETED:
        break;
    }
    cn->mutex.Unlock();
  }

  if (error) {
    spdlog::debug("Some error take place with the config namespace '{}'", cn->root_path);
    auto output_message = request->make_output_message();
    output_message->set_namespace_id(cn->id);
    output_message->set_status(api::stream::WatchStatus::ERROR);
    output_message->commit();
    return false;
  }

  for_each_trace_to_trigger(
    cn.get(),
    request.get(),
    [version, cn=cn.get(), request=request.get()](auto* trace) {
      auto om = make_trace_output_message(
        trace,
        api::stream::TraceOutputMessage::Status::ADDED_WATCHER,
        cn->id,
        version,
        request
      );
      om->commit();
    }
  );

  auto output_message = request->make_output_message();

  return process_get_config_task(
    std::move(cn),
    std::make_shared<ApiGetConfigTask>(
      std::make_shared<api::stream::WatchGetRequest>(
        version,
        std::move(request),
        std::move(output_message)
      )
    ),
    ctx
  );
}

bool process_trace_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::stream::TraceInputMessage>&& request,
  context_t* ctx
) {
  spdlog::debug("Processing the trace request {}", (void*) request.get());

  bool error = true;
  bool require_exclusive_lock = false;

  cn->mutex.ReaderLock();
  switch (cn->status) {
    case ConfigNamespaceStatus::UNDEFINED: // Fallback
    case ConfigNamespaceStatus::BUILDING:
      require_exclusive_lock = true;
      break;
    case ConfigNamespaceStatus::OK: // Fallback
    case ConfigNamespaceStatus::OK_UPDATING:
      error = false;
      break;
    case ConfigNamespaceStatus::DELETED:
      break;
  }
  cn->mutex.ReaderUnlock();

  if (require_exclusive_lock) {
    cn->mutex.Lock();
    switch (cn->status) {
      case ConfigNamespaceStatus::UNDEFINED: {
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = request->root_path();
        cn->trace_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();

        spdlog::debug(
          "The config namespace '{}' is undefined, setting it!",
          cn->root_path
        );

        execute_command_in_worker_thread(
          std::make_unique<worker::SetupCommand>(std::move(cn)),
          ctx
        );

        return true;
      }
      case ConfigNamespaceStatus::BUILDING:
        cn->trace_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();
        spdlog::debug("The config namespace '{}' is building, waiting it!", cn->root_path);
        return true;
      case ConfigNamespaceStatus::OK: // Fallback
      case ConfigNamespaceStatus::OK_UPDATING:
        error = false;
        break;
      case ConfigNamespaceStatus::DELETED:
        break;
    }
    cn->mutex.Unlock();
  }

  if (error) {
    spdlog::debug("Some error take place with the config namespace '{}'", cn->root_path);
    auto om = request->make_output_message();
    om->set_namespace_id(cn->id);
    om->set_status(api::stream::TraceOutputMessage::Status::ERROR);
    om->send(true);
    return false;
  }

  bool trace_labels = !request->labels().empty();
  bool trace_document = !request->document().empty();

  absl::flat_hash_set<label_t> labels;

  if (!trace_labels && !trace_document) {
    // In this case we want to trace all the requests
    cn->to_trace_always.add(request);
  } else {
    for (const auto& label : request->labels()) {
      cn->traces_by_label.insert(label, request);
      labels.insert(label);
    }

    if (trace_document) {
      cn->traces_by_document.insert(request->document(), request);
    }
  }

  cn->mutex.ReaderLock();
  if (trace_document) {
    if (
      auto search = cn->document_versions_by_name.find(request->document());
      search != cn->document_versions_by_name.end()
    ) {
      send_existing_watcher_traces(
        cn.get(),
        search->second.get(),
        request.get(),
        labels
      );
    }
  } else {
    for (auto& it : cn->document_versions_by_name) {
      send_existing_watcher_traces(
        cn.get(),
        it.second.get(),
        request.get(),
        labels
      );
    }
  }
  cn->mutex.ReaderUnlock();

  return true;
}

} /* mhconfig */
