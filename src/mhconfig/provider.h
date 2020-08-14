#ifndef MHCONFIG__PROVIDER_H
#define MHCONFIG__PROVIDER_H

#include "mhconfig/config_namespace.h"
#include "mhconfig/builder.h"
#include "mhconfig/worker/optimize_command.h"
#include "mhconfig/worker/build_command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/api/stream/trace_stream.h"

namespace mhconfig
{

template <typename SetupCommand>
bool process_get_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::request::GetRequest>&& request,
  context_t* ctx
) {
  spdlog::debug("Processing the get request {}", (void*) request.get());

  VersionId version = request->version();
  std::shared_ptr<document_t> document;

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
      version = get_version(cn.get(), version);
      if (version != 0) {
        document = get_document_locked(cn.get(), request->document(), version);
      }
      error = false;
      break;
    case ConfigNamespaceStatus::DELETED:
      break;
  }
  cn->mutex.ReaderUnlock();

  if (require_exclusive_lock) {
    cn->mutex.Lock();
    switch (cn->status) {
      case ConfigNamespaceStatus::UNDEFINED:
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = request->root_path();
        cn->get_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();

        spdlog::debug("The config namespace '{}' is undefined, setting it!", cn->root_path);
        ctx->worker_queue.push(std::make_unique<SetupCommand>(std::move(cn)));

        return true;
      case ConfigNamespaceStatus::BUILDING:
        cn->get_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();
        spdlog::debug("The config namespace '{}' is building, waiting it!", cn->root_path);
        return true;
      case ConfigNamespaceStatus::OK: // Fallback
      case ConfigNamespaceStatus::OK_UPDATING:
        version = get_version(cn.get(), version);
        if (version != 0) {
          document = get_document_locked(cn.get(), request->document(), version);
        }
        error = false;
        break;
      case ConfigNamespaceStatus::DELETED:
        break;
    }
    cn->mutex.Unlock();
  }

  request->set_namespace_id(cn->id);

  if (error) {
    spdlog::debug("Some error take place with the config namespace '{}'", cn->root_path);
    request->set_status(api::request::GetRequest::Status::ERROR);
    request->commit();
    return false;
  }

  if (version == 0) {
    spdlog::error("The asked version {} don't exists", request->version());
    request->set_status(api::request::GetRequest::Status::INVALID_VERSION);
    request->commit();
    return true;
  }

  request->set_version(version);

  if (document == nullptr) {
    spdlog::error(
      "The asked document '{}' with the version {} don't exists",
      request->document(),
      version
    );
    request->set_element(UNDEFINED_ELEMENT);
    request->commit();
    return true;
  }

  std::string overrides_key;
  overrides_key.reserve(
    (request->flavors().size()+1) * request->overrides().size() * sizeof(RawConfigId)
  );

  bool is_a_valid_version = for_each_document_override(
    document.get(),
    request->flavors(),
    request->overrides(),
    version,
    [&overrides_key](const auto&, auto& raw_config) {
      jmutils::push_uint16(overrides_key, raw_config->id);
    }
  );

  if (!is_a_valid_version) {
    spdlog::error("The asked version {} don't exists", request->version());
    request->set_status(api::request::GetRequest::Status::INVALID_VERSION);
    request->commit();
    return true;
  }

  for_each_trace_to_trigger(
    cn.get(),
    request.get(),
    [version, cn=cn.get(), request=request.get()](auto* trace) {
      auto om = make_trace_output_message(
        trace,
        api::stream::TraceOutputMessage::Status::RETURNED_ELEMENTS,
        cn->id,
        version,
        request
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
  check_merged_config_result = check_merged_config<false>(merged_config.get(), request);
  merged_config->mutex.ReaderUnlock();

  switch (check_merged_config_result) {
    case CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK:
      spdlog::trace("The operation need a exclusive lock");
      break;
    case CheckMergedConfigResult::COMMIT_MESSAGE:
      spdlog::trace("Commiting the message");
      request->commit();
      return true;
    default:
      spdlog::trace("Some error take place");
      return false;
  }

  merged_config->mutex.Lock();
  check_merged_config_result = check_merged_config<true>(merged_config.get(), request);
  merged_config->mutex.Unlock();

  switch (check_merged_config_result) {
    case CheckMergedConfigResult::IN_PROGRESS:
      spdlog::trace("The operation is in progress");
      return true;
    case CheckMergedConfigResult::BUILD_CONFIG: {
      spdlog::trace("The operation need build the config");

      auto pending_build = std::make_shared<pending_build_t>();
      pending_build->version = version;
      pending_build->request = std::move(request);
      pending_build->element = std::make_unique<build_element_t>();
      pending_build->element->document = std::move(document);
      pending_build->element->merged_config = std::move(merged_config);

      ctx->worker_queue.push(
        std::make_unique<worker::BuildCommand>(
          std::move(cn),
          std::move(pending_build)
        )
      );

      return true;
    }
    case CheckMergedConfigResult::OPTIMIZE_CONFIG:
      spdlog::trace("Optimizing a commiting the message");
      ctx->worker_queue.push(
        std::make_unique<worker::OptimizeCommand>(
          std::move(merged_config)
        )
      );
      return true;
    case CheckMergedConfigResult::COMMIT_MESSAGE:
      spdlog::trace("Commiting the message");
      request->commit();
      return true;
  }

  spdlog::trace("Some error take place");
  return false;
}

template <typename SetupCommand, typename UpdateCommand>
bool process_update_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::request::UpdateRequest>&& request,
  context_t* ctx
) {
  spdlog::debug("Processing the update request {}", (void*) request.get());

  cn->mutex.Lock();
  switch (cn->status) {
    case ConfigNamespaceStatus::UNDEFINED:
      cn->status = ConfigNamespaceStatus::BUILDING;
      cn->root_path = request->root_path();
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();

      spdlog::debug("The config namespace '{}' is undefined, setting it!", cn->root_path);
      ctx->worker_queue.push(std::make_unique<SetupCommand>(std::move(cn)));

      return true;
    case ConfigNamespaceStatus::BUILDING: // Fallback
    case ConfigNamespaceStatus::OK_UPDATING:
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();
      spdlog::debug("The config namespace '{}' isn't ready, waiting it!", cn->root_path);
      return true;
    case ConfigNamespaceStatus::OK:
      cn->status = ConfigNamespaceStatus::OK_UPDATING;
      cn->update_requests_waiting.push_back(std::move(request));
      cn->mutex.Unlock();

      spdlog::debug("To update the config namespace '{}'", cn->root_path);
      ctx->worker_queue.push(std::make_unique<UpdateCommand>(std::move(cn)));

      return true;
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

template <typename SetupCommand, typename UpdateCommand, typename WatchGetRequest>
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
      case ConfigNamespaceStatus::UNDEFINED:
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = request->root_path();
        cn->watch_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();

        spdlog::debug("The config namespace '{}' is undefined, setting it!", cn->root_path);
        ctx->worker_queue.push(std::make_unique<SetupCommand>(std::move(cn)));

        return true;
      case ConfigNamespaceStatus::BUILDING:
      case ConfigNamespaceStatus::OK_UPDATING:
        cn->watch_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();
        spdlog::debug("The config namespace '{}' isn't prepared, waiting it!", cn->root_path);
        return true;
      case ConfigNamespaceStatus::OK: {
        auto dv = get_or_guild_document_versions_locked(cn.get(), request->document());
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

  return process_get_request<SetupCommand>(
    std::move(cn),
    std::make_shared<WatchGetRequest>(
      version,
      std::move(request),
      std::move(output_message)
    ),
    ctx
  );
}

void send_existing_watcher_traces(
  config_namespace_t* cn,
  document_versions_t* dv,
  api::stream::TraceInputMessage* trace,
  absl::flat_hash_set<std::string>& overrides,
  absl::flat_hash_set<std::string>& flavors
);

template <typename SetupCommand>
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
      case ConfigNamespaceStatus::UNDEFINED:
        cn->status = ConfigNamespaceStatus::BUILDING;
        cn->root_path = request->root_path();
        cn->trace_requests_waiting.push_back(std::move(request));
        cn->mutex.Unlock();

        spdlog::debug("The config namespace '{}' is undefined, setting it!", cn->root_path);
        ctx->worker_queue.push(std::make_unique<SetupCommand>(std::move(cn)));

        return true;
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

  bool trace_overrides = !request->overrides().empty();
  bool trace_flavors = !request->flavors().empty();
  bool trace_document = !request->document().empty();

  absl::flat_hash_set<std::string> overrides;
  absl::flat_hash_set<std::string> flavors;

  if (!trace_overrides && !trace_flavors && !trace_document) {
    // In this case we want to trace all the requests
    cn->to_trace_always.add(request);
  } else {
    for (const auto& override_ : request->overrides()) {
      cn->traces_by_override.insert(override_, request);
      overrides.insert(override_);
    }

    for (const auto& flavor : request->flavors()) {
      cn->traces_by_flavor.insert(flavor, request);
      flavors.insert(flavor);
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
      send_existing_watcher_traces(cn.get(), search->second.get(), request.get(), overrides, flavors);
    }
  } else {
    for (auto& it : cn->document_versions_by_name) {
      send_existing_watcher_traces(cn.get(), it.second.get(), request.get(), overrides, flavors);
    }
  }
  cn->mutex.ReaderUnlock();

  return true;
}

} /* mhconfig */

#endif
